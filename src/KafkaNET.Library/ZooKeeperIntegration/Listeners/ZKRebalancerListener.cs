/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


namespace Kafka.Client.ZooKeeperIntegration.Listeners
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration.Events;
    using log4net;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using ZooKeeperNet;

    internal class ZKRebalancerListener<TData> : IZooKeeperChildListener
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger("ZKRebalancerListener");

        public event EventHandler ConsumerRebalance;

        private IDictionary<string, IList<string>> oldPartitionsPerTopicMap = new Dictionary<string, IList<string>>();
        private IDictionary<string, IList<string>> oldConsumersPerTopicMap = new Dictionary<string, IList<string>>();
        private IDictionary<string, IDictionary<int, PartitionTopicInfo>> topicRegistry;
        private readonly IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> queues;
        private readonly string consumerIdString;
        private readonly object syncLock = new object();
        private readonly object asyncLock = new object();
        private readonly ConsumerConfiguration config;
        private readonly IZooKeeperClient zkClient;
        private readonly ZKGroupDirs dirs;
        private readonly Fetcher fetcher;
        private readonly ZookeeperConsumerConnector zkConsumerConnector;
        private readonly IDictionary<string, IList<KafkaMessageStream<TData>>> kafkaMessageStreams;
        private readonly TopicCount topicCount;
        private volatile CancellationTokenSource rebalanceCancellationTokenSource = new CancellationTokenSource();
        private volatile bool isRebalanceRunning = false;

        internal ZKRebalancerListener(
            ConsumerConfiguration config,
            string consumerIdString,
            IDictionary<string, IDictionary<int, PartitionTopicInfo>> topicRegistry,
            IZooKeeperClient zkClient,
            ZookeeperConsumerConnector zkConsumerConnector,
            IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> queues,
            Fetcher fetcher,
            IDictionary<string, IList<KafkaMessageStream<TData>>> kafkaMessageStreams,
            TopicCount topicCount)
        {
            this.consumerIdString = consumerIdString;
            this.config = config;
            this.topicRegistry = topicRegistry;
            this.zkClient = zkClient;
            this.dirs = new ZKGroupDirs(config.GroupId);
            this.zkConsumerConnector = zkConsumerConnector;
            this.queues = queues;
            this.fetcher = fetcher;
            this.kafkaMessageStreams = kafkaMessageStreams;
            this.topicCount = topicCount;
        }

        /// <summary>
        /// Called when the children of the given path changed
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperChildChangedEventArgs"/> instance containing the event data
        /// as parent path and children (null if parent was deleted).
        /// </param>
        /// <remarks> 
        /// http://zookeeper.wiki.sourceforge.net/ZooKeeperWatches
        /// </remarks>
        public void HandleChildChange(ZooKeeperChildChangedEventArgs args)
        {
            Guard.NotNull(args, "args");
            Guard.NotNullNorEmpty(args.Path, "args.Path");
            Guard.NotNull(args.Children, "args.Children");
            Logger.Info("Performing rebalancing. Consumers have been added or removed from the consumer group: " + args.Path);
            AsyncRebalance();
        }

        /// <summary>
        /// Resets the state of listener.
        /// </summary>
        public void ResetState()
        {
            Logger.Info("enter ResetState ...");
            try
            {
                zkClient.SlimLock.EnterWriteLock();
                this.topicRegistry.Clear();
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("error in ResetState : {0}", ex.FormatException());
            }
            finally
            {
                zkClient.SlimLock.ExitWriteLock();
            }
            this.oldConsumersPerTopicMap.Clear();
            this.oldPartitionsPerTopicMap.Clear();
            Logger.Info("finish ResetState.");
        }

        public void AsyncRebalance(int waitRebalanceFinishTimeoutInMs = 0)
        {
            lock (this.asyncLock)
            {
                // Stop currently running rebalance
                StopRebalance();

                // Run new rebalance operation asynchronously
                Logger.Info("Asynchronously running rebalance operation");
                Task.Factory.StartNew(() => SyncedRebalance(rebalanceCancellationTokenSource));

                // Set flag to indicate that rebalance is running
                isRebalanceRunning = true;
            }

            if (waitRebalanceFinishTimeoutInMs > 0)
            {
                DateTime timeStartWait = DateTime.UtcNow;
                while (isRebalanceRunning == true)
                {
                    Logger.Info("Waiting for rebalance operation finish");
                    Thread.Sleep(100);
                    if ((DateTime.UtcNow - timeStartWait).TotalMilliseconds > waitRebalanceFinishTimeoutInMs)
                    {
                        Logger.WarnFormat("After wait more than {0} ms, the latest isRebalanceRunning:{1} ", waitRebalanceFinishTimeoutInMs, isRebalanceRunning);
                        break;
                    }
                }
                Logger.InfoFormat("Finish Wait for rebalance operation. isRebalanceRunning:{0}", isRebalanceRunning);
            }
        }

        public void StopRebalance()
        {
            // Pulse cancellation token to stop currently running rebalance operation
            Logger.Info("Pulsing cancellation token to stop currently running rebalance operation");
            rebalanceCancellationTokenSource.Cancel();

            // Wait until rebalance operation has successfully terminated
            while (isRebalanceRunning == true)
            {
                Logger.Info("Waiting for rebalance operation to successfully terminate");
                Thread.Sleep(1000);
            }

            // Now it is safe to reset cancellation token source
            Logger.Info("Create cancellation token");
            rebalanceCancellationTokenSource = new CancellationTokenSource();
        }

        public void ShutdownFetchers()
        {
            if (this.fetcher != null)
            {
                this.fetcher.Shutdown();
            }
            foreach (KeyValuePair<string, IDictionary<int, PartitionTopicInfo>> topic in topicRegistry)
            {
                foreach (KeyValuePair<int, PartitionTopicInfo> partition in topic.Value)
                {
                    partition.Value.ConsumeOffsetValid = false;
                }
            }
        }

        protected virtual void OnConsumerRebalance(EventArgs args)
        {
            try
            {
                EventHandler handler = ConsumerRebalance;
                if (handler != null)
                {
                    handler(this, args);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("Exception occurred within event handler for ConsumerRebalance event: " + ex.Message);
            }
        }

        private void SyncedRebalance(CancellationTokenSource cancellationTokenSource)
        {
            Logger.InfoFormat("Consumer {0} has entered rebalance", consumerIdString);

            lock (this.syncLock)
            {
                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    // Notify listeners that a rebalance is occurring
                    OnConsumerRebalance(EventArgs.Empty);

                    Logger.InfoFormat("Begin rebalancing consumer {0}", consumerIdString);
                    try
                    {
                        // Query ZooKeeper for current broker metadata
                        var cluster = new Cluster(zkClient);

                        // Begin consumer rebalance
                        if (Rebalance(cluster, cancellationTokenSource))
                        {
                            Logger.InfoFormat("End rebalancing consumer {0}", consumerIdString);
                            break;
                        }
                        else
                        {
                            Logger.ErrorFormat("Rebalance return false, will retry.  ");
                        }
                    }
                    catch (ObjectDisposedException objDisposedEx)
                    {
                        // some lower methods have methods like EnsureNotDisposed() implemented and so we should expect to catch here
                        Logger.InfoFormat("ObjectDisposedException in rebalance. Assume end of SyncedRebalance. Exception occurred during rebalance {1} for consumer {2}", this.config.ConsumeGroupRebalanceRetryIntervalMs, objDisposedEx.FormatException(), consumerIdString);
                        cancellationTokenSource.Cancel();
                    }
                    catch (Exception ex)
                    {
                        // Some unknown exception occurred, bail
                        Logger.ErrorFormat("Exception in rebalance. Will retry after {0} ms. Exception occurred during rebalance {1} for consumer {2}", this.config.ConsumeGroupRebalanceRetryIntervalMs, ex.FormatException(), consumerIdString);
                        Thread.Sleep(this.config.ConsumeGroupRebalanceRetryIntervalMs);
                        continue;
                    }
                }
                // Clear flag on exit to indicate rebalance has completed
                isRebalanceRunning = false;
            }

            Logger.InfoFormat("Consumer {0} has exited rebalance", consumerIdString);
        }

        private bool Rebalance(Cluster cluster, CancellationTokenSource cancellationTokenSource)
        {
            TopicCount topicCount = this.GetTopicCount(this.consumerIdString);
            IDictionary<string, IList<string>> topicThreadIdsMap = topicCount.GetConsumerThreadIdsPerTopic();
            if (!topicThreadIdsMap.Any())
            {
                Logger.ErrorFormat("Consumer ID is not registered to any topics in ZK. Exiting rebalance");
                return false;
            }
            var consumersPerTopicMap = this.GetConsumersPerTopic(this.config.GroupId);

            var brokers = ZkUtils.GetAllBrokersInCluster(zkClient);
            if (!brokers.Any())
            {
                Logger.Warn("No brokers found when trying to rebalance.");
                zkClient.Subscribe(ZooKeeperClient.DefaultBrokerIdsPath, this);
                this.zkConsumerConnector.subscribedChildCollection.Add(new Tuple<string, IZooKeeperChildListener>(ZooKeeperClient.DefaultBrokerIdsPath, this));
                Logger.ErrorFormat("Subscribe count: subscribedChildCollection:{0} , subscribedZookeeperStateCollection:{1} subscribedZookeeperDataCollection:{2} "
                     , this.zkConsumerConnector.subscribedChildCollection.Count, this.zkConsumerConnector.subscribedZookeeperStateCollection.Count, this.zkConsumerConnector.subscribedZookeeperDataCollection.Count);
                return false;
            }

            var partitionsPerTopicMap = ZkUtils.GetPartitionsForTopics(this.zkClient, topicThreadIdsMap.Keys);

            // Check if we've been canceled externally before we dive into the rebalance
            if (cancellationTokenSource.IsCancellationRequested)
            {
                Logger.ErrorFormat("Rebalance operation has been canceled externally by a future rebalance event. Exiting immediately");
                return false;
            }

            this.CloseFetchers(cluster, topicThreadIdsMap, this.zkConsumerConnector);
            this.ReleasePartitionOwnership(topicThreadIdsMap);

            try
            {
                foreach (var item in topicThreadIdsMap)
                {
                    var topic = item.Key;
                    var consumerThreadIdSet = item.Value;

                    topicRegistry.Add(topic, new ConcurrentDictionary<int, PartitionTopicInfo>());

                    var topicDirs = new ZKGroupTopicDirs(config.GroupId, topic);
                    List<string> curConsumers = new List<string>(consumersPerTopicMap[topic]);
                    curConsumers.Sort();

                    List<string> curPartitions = partitionsPerTopicMap[topic].OrderBy(p => int.Parse(p)).ToList();

                    Logger.InfoFormat(
                        "{4} Partitions. {5} ConsumerClients.  Consumer {0} rebalancing the following partitions: {1} for topic {2} with consumers: {3}",
                        this.consumerIdString,
                        string.Join(",", curPartitions),
                        topic,
                        string.Join(",", curConsumers),
                        curPartitions.Count,
                        curConsumers.Count);

                    var numberOfPartsPerConsumer = curPartitions.Count / curConsumers.Count;
                    Logger.Info("Number of partitions per consumer is: " + numberOfPartsPerConsumer);

                    var numberOfConsumersWithExtraPart = curPartitions.Count % curConsumers.Count;
                    Logger.Info("Number of consumers with an extra partition are: " + numberOfConsumersWithExtraPart);

                    foreach (string consumerThreadId in consumerThreadIdSet)
                    {
                        var myConsumerPosition = curConsumers.IndexOf(consumerThreadId);
                        Logger.Info("Consumer position for consumer " + consumerThreadId + " is: " + myConsumerPosition);

                        if (myConsumerPosition < 0)
                        {
                            continue;
                        }

                        var startPart = (numberOfPartsPerConsumer * myConsumerPosition) +
                                        Math.Min(myConsumerPosition, numberOfConsumersWithExtraPart);
                        Logger.Info("Starting partition is: " + startPart);

                        var numberOfParts = numberOfPartsPerConsumer + (myConsumerPosition + 1 > numberOfConsumersWithExtraPart ? 0 : 1);
                        Logger.Info("Number of partitions to work on is: " + numberOfParts);

                        if (numberOfParts <= 0)
                        {
                            Logger.InfoFormat("No broker partitions consumed by consumer thread {0} for topic {1}", consumerThreadId, item.Key);
                        }
                        else
                        {
                            for (int i = startPart; i < startPart + numberOfParts; i++)
                            {
                                var partition = curPartitions[i];

                                Logger.InfoFormat("{0} attempting to claim partition {1}", consumerThreadId, partition);
                                bool ownPartition = ProcessPartition(topicDirs, partition, topic, consumerThreadId, curConsumers, curPartitions, cancellationTokenSource);
                                if (!ownPartition)
                                {
                                    Logger.InfoFormat("{0} failed to claim partition {1} for topic {2}. Exiting rebalance", consumerThreadId, partition, topic);
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("error when rebalance: {0}", ex.FormatException());
                return false;
            }

            // If we get here, we know that we have owned all partitions successfully,
            // therefore it is safe to update fetcher threads and begin dequeuing
            Logger.Info("All partitions were successfully owned. Updating fetchers");

            this.UpdateFetcher(cluster);

            return true;
        }

        private bool ProcessPartition(ZKGroupTopicDirs topicDirs,
                                      string partition,
                                      string topic,
                                      string consumerThreadId,
                                      List<string> curConsumers,
                                      List<string> curPartitions, CancellationTokenSource cancellationTokenSource)
        {
            bool partitionOwned = false;

            // Loop until we can successfully acquire partition, or are canceled by a
            // future rebalance event
            var partitionOwnerPath = topicDirs.ConsumerOwnerDir + "/" + partition;
            while (true)
            {
                var currentPartitionOwner = zkClient.ReadData<string>(partitionOwnerPath, true);
                if (currentPartitionOwner == null || currentPartitionOwner.Equals(consumerThreadId))
                {
                    Logger.InfoFormat("{0} is null or equals our consumer ID. Reflect partition ownership in ZK", partitionOwnerPath);
                    AddPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId);
                    if (ReflectPartitionOwnershipDecision(topic, partition, consumerThreadId))
                    {
                        partitionOwned = true;
                        break;
                    }
                }

                if (cancellationTokenSource.IsCancellationRequested)
                {
                    Logger.Info("Rebalance operation has been canceled externally by a future rebalance event. Exiting immediately");
                    break;
                }
                else
                {
                    Logger.InfoFormat("{0} exists with value {1}. Sleep and retry ownership attempt", partitionOwnerPath, currentPartitionOwner);
                    Thread.Sleep(1000);
                }
            }

            return partitionOwned;
        }

        private bool ReflectPartitionOwnershipDecision(string topic, string partition, string consumerThreadId)
        {
            bool ownershipReflected = false;
            var topicDirs = new ZKGroupTopicDirs(config.GroupId, topic);
            var partitionOwnerPath = topicDirs.ConsumerOwnerDir + "/" + partition;

            try
            {
                Logger.InfoFormat("Consumer {0} will own partition {1} for topic {2}, will create ZK path {3}", consumerThreadId, partition, topic, partitionOwnerPath);
                try
                {
                    zkClient.SlimLock.EnterWriteLock();
                    ZkUtils.CreateEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
                    Logger.InfoFormat("Consumer {0} SUCC owned partition {1} for topic {2} . finish create ZK path {3}  .", consumerThreadId, partition, topic, partitionOwnerPath);
                    ownershipReflected = true;
                }
                catch (Exception ex)
                {
                    Logger.InfoFormat("Consumer {0} FAILED owned partition {1} for topic {2} . finish create ZK path {3}  error:{4}.", consumerThreadId, partition, topic, partitionOwnerPath, ex.FormatException());
                }
                finally
                {
                    zkClient.SlimLock.ExitWriteLock();
                }
            }
            catch (KeeperException e)
            {
                if (e.ErrorCode == KeeperException.Code.NODEEXISTS)
                {
                    Logger.InfoFormat("{0} failed to own partition {1} for topic {2}. Waiting for the partition owner to be deleted", consumerThreadId, partition, topic);
                    ownershipReflected = false;
                }
                else
                    throw;
            }

            return ownershipReflected;
        }

        private void CloseFetchers(Cluster cluster, IDictionary<string, IList<string>> relevantTopicThreadIdsMap, ZookeeperConsumerConnector zkConsumerConnector)
        {
            Logger.Info("enter CloseFetchers ...");
            var queuesToBeCleared = queues.Where(q => relevantTopicThreadIdsMap.ContainsKey(q.Key.Item1)).Select(q => q.Value).ToList();
            CloseFetchersForQueues(cluster, queuesToBeCleared, this.kafkaMessageStreams, zkConsumerConnector);
            Logger.Info("exit CloseFetchers");
        }

        private void UpdateFetcher(Cluster cluster)
        {
            var allPartitionInfos = new List<PartitionTopicInfo>();
            foreach (var partitionInfos in this.topicRegistry.Values)
            {
                foreach (var partition in partitionInfos.Values)
                {
                    allPartitionInfos.Add(partition);
                }
            }
            Logger.InfoFormat("Consumer {0} selected partitions : {1}", consumerIdString,
                              string.Join(",", allPartitionInfos.OrderBy(x => x.PartitionId).Select(y => y.ToString())));
            if (this.fetcher != null)
            {
                this.fetcher.InitConnections(allPartitionInfos, cluster);
            }
        }

        private void AddPartitionTopicInfo(ZKGroupTopicDirs topicDirs, string partition, string topic, string consumerThreadId)
        {
            var partitionId = int.Parse(partition);
            var partTopicInfoMap = this.topicRegistry[topic];

            //find the leader for this partition
            var leaderOpt = ZkUtils.GetLeaderForPartition(this.zkClient, topic, partitionId);
            if (!leaderOpt.HasValue)
            {
                throw new NoBrokersForPartitionException(string.Format("No leader available for partitions {0} on topic {1}", partition, topic));
            }
            else
            {
                Logger.InfoFormat("Leader for partition {0} for topic {1} is {2}", partition, topic, leaderOpt.Value);
            }
            var leader = leaderOpt.Value;
            var znode = topicDirs.ConsumerOffsetDir + "/" + partition;

            var offsetCommitedString = this.zkClient.ReadData<string>(znode, true);

            //if first time starting a consumer, set the initial offset based on the config
            long offset = -1;
            long offsetCommited = -1;
            if (offsetCommitedString != null)
            {
                offsetCommited = long.Parse(offsetCommitedString);
                offset = offsetCommited + 1;
            }
            Logger.InfoFormat("Final offset {0} for topic {1} partition {2} OffsetCommited {3}"
                    , offset, topic, partition, offsetCommited);

            var queue = this.queues[new Tuple<string, string>(topic, consumerThreadId)];
            var partTopicInfo = new PartitionTopicInfo(
                topic,
                leader,
                partitionId,
                queue,
                offsetCommited,
                offset,
                this.config.FetchSize,
                offsetCommited);
            partTopicInfoMap[partitionId] = partTopicInfo;
            Logger.InfoFormat("{0} selected new offset {1}", partTopicInfo, offset);
        }

        private void ReleasePartitionOwnership(IDictionary<string, IList<string>> topicThreadIdsMap)
        {

            try
            {
                Logger.Info("Will release partition ownership");

                this.zkConsumerConnector.ReleaseAllPartitionOwnerships();

                Logger.Info("Will clean up topicRegistry ...");
                foreach (var item in topicThreadIdsMap)
                {
                    var topic = item.Key;
                    bool removeTopic = topicRegistry.Remove(topic);
                    Logger.InfoFormat("clean up topicRegistry result for topic {0} is {1} ", topic, removeTopic);
                }
                Logger.Info("Finish clean up topicRegistry");
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("error in ReleasePartitionOwnership : {0}", ex.FormatException());
            }
            Logger.Info("Finish release partition ownership");
        }

        private TopicCount GetTopicCount(string consumerId)
        {
            var topicCountJson = this.zkClient.ReadData<string>(this.dirs.ConsumerRegistryDir + "/" + consumerId);
            return TopicCount.ConstructTopicCount(consumerId, topicCountJson);
        }

        private IDictionary<string, IList<string>> GetConsumersPerTopic(string group)
        {
            var consumers = this.zkClient.GetChildrenParentMayNotExist(this.dirs.ConsumerRegistryDir);
            var consumersPerTopicMap = new Dictionary<string, IList<string>>();
            foreach (var consumer in consumers)
            {
                TopicCount topicCount = GetTopicCount(consumer);
                foreach (KeyValuePair<string, IList<string>> consumerThread in topicCount.GetConsumerThreadIdsPerTopic())
                {
                    foreach (string consumerThreadId in consumerThread.Value)
                    {
                        if (!consumersPerTopicMap.ContainsKey(consumerThread.Key))
                        {
                            consumersPerTopicMap.Add(consumerThread.Key, new List<string> { consumerThreadId });
                        }
                        else
                        {
                            consumersPerTopicMap[consumerThread.Key].Add(consumerThreadId);
                        }
                    }
                }
            }

            foreach (KeyValuePair<string, IList<string>> item in consumersPerTopicMap)
            {
                item.Value.ToList().Sort();
            }

            return consumersPerTopicMap;
        }

        private void CloseFetchersForQueues(Cluster cluster, IEnumerable<BlockingCollection<FetchedDataChunk>> queuesToBeCleared, IDictionary<string, IList<KafkaMessageStream<TData>>> kafkaMessageStreams, ZookeeperConsumerConnector zkConsumerConnector)
        {
            if (this.fetcher != null)
            {
                var allPartitionInfos = new List<PartitionTopicInfo>();
                foreach (var item in this.topicRegistry.Values)
                {
                    foreach (var partitionTopicInfo in item.Values)
                    {
                        allPartitionInfos.Add(partitionTopicInfo);
                    }
                }
                fetcher.Shutdown();
                fetcher.ClearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, kafkaMessageStreams);
                Logger.Info("Committing all offsets after clearing the fetcher queues");

                if (this.config.AutoCommit)
                {
                    zkConsumerConnector.CommitOffsets();
                }
            }
        }
    }
}