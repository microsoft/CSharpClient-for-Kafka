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

namespace Kafka.Client.Consumers
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using Kafka.Client.ZooKeeperIntegration.Listeners;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// The consumer high-level API, that hides the details of brokers from the consumer. 
    /// It also maintains the state of what has been consumed. 
    /// </summary>
    public class ZookeeperConsumerConnector : KafkaClientBase, IZookeeperConsumerConnector
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ZookeeperConsumerConnector));
        public static readonly int MaxNRetries = 4;
        public static bool UseSharedStaticZookeeperClient = true;
        public static volatile IZooKeeperClient zkClientStatic;
        public static object zkClientStaticLock = new object();

        internal static readonly FetchedDataChunk ShutdownCommand = new FetchedDataChunk(null, null, -1);
        private static readonly int DefaultWaitTimeForInitialRebalanceInSeconds = 60;

        internal ConcurrentBag<Tuple<string, IZooKeeperChildListener>> subscribedChildCollection = new ConcurrentBag<Tuple<string, IZooKeeperChildListener>>();
        internal ConcurrentBag<IZooKeeperStateListener> subscribedZookeeperStateCollection = new ConcurrentBag<IZooKeeperStateListener>();
        internal ConcurrentBag<Tuple<string, IZooKeeperDataListener>> subscribedZookeeperDataCollection = new ConcurrentBag<Tuple<string, IZooKeeperDataListener>>();

        private readonly ConsumerConfiguration config;
        private volatile IZooKeeperClient zkClientInternal;
        private readonly object shuttingDownLock = new object();
        private readonly bool enableFetcher;
        private Fetcher fetcher;
        private readonly KafkaScheduler scheduler = new KafkaScheduler();
        private readonly IDictionary<string, IDictionary<int, PartitionTopicInfo>> topicRegistry = new ConcurrentDictionary<string, IDictionary<int, PartitionTopicInfo>>();
        private readonly IDictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>> queues = new Dictionary<Tuple<string, string>, BlockingCollection<FetchedDataChunk>>();
        private List<Action> stopAsyncRebalancing = new List<Action>();
        private volatile bool disposed;
        private EventHandler consumerRebalanceHandler;
        private EventHandler zkSessionDisconnectedHandler;
        private EventHandler zkSessionExpiredHandler;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZookeeperConsumerConnector"/> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration. At the minimum, need to specify the group ID 
        /// of the consumer and the ZooKeeper connection string.
        /// </param>
        /// <param name="enableFetcher">
        /// Indicates whether fetchers should be enabled
        /// </param>
        public ZookeeperConsumerConnector(ConsumerConfiguration config,
                                          bool enableFetcher,
                                          EventHandler rebalanceHandler = null,
                                          EventHandler zkDisconnectedHandler = null,
                                          EventHandler zkExpiredHandler = null)
        {
            if (string.IsNullOrEmpty(config.GroupId))
            {
                throw new ArgumentNullException("GroupId of ConsumerConfiguration should not be empty.");
            }
            Logger.Info("Enter ZookeeperConsumerConnector ...");
            try
            {
                this.config = config;
                this.enableFetcher = enableFetcher;
                Logger.Info("ZookeeperConsumerConnector construct will connect zk ...");
                this.ConnectZk();
                Logger.Info("ZookeeperConsumerConnector construct After connect zk ...");
                Logger.Info("ZookeeperConsumerConnector construct will CreateFetcher ...");
                this.CreateFetcher();
                Logger.Info("ZookeeperConsumerConnector construct After CreateFetcher ...");
                this.consumerRebalanceHandler = rebalanceHandler;
                this.zkSessionDisconnectedHandler = zkDisconnectedHandler;
                this.zkSessionExpiredHandler = zkExpiredHandler;

                if (this.config.AutoCommit)
                {
                    Logger.InfoFormat("starting auto committer every {0} ms", this.config.AutoCommitInterval);
                    scheduler.ScheduleWithRate(this.AutoCommit, this.config.AutoCommitInterval, this.config.AutoCommitInterval);
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException("ZookeeperConsumerConnector exception: " + ex.FormatException());
            }
            Logger.Info("Exit ZookeeperConsumerConnector ...");
        }

        /// <summary>
        /// Gets the consumer group ID.
        /// </summary>
        public string ConsumerGroup
        {
            get { return this.config.GroupId; }
        }

        /// <summary>
        /// Gets the current ownership.
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, IDictionary<int, PartitionTopicInfo>> GetCurrentOwnership()
        {
            return new ConcurrentDictionary<string, IDictionary<int, PartitionTopicInfo>>(topicRegistry);
        }

        /// <summary>
        /// Commits the offsets of all messages consumed so far.
        /// </summary>
        public void CommitOffsets()
        {
            this.EnsuresNotDisposed();
            if (this.GetZkClient() == null)
            {
                return;
            }
            try
            {
                foreach (KeyValuePair<string, IDictionary<int, PartitionTopicInfo>> topic in topicRegistry)
                {
                    var topicDirs = new ZKGroupTopicDirs(this.config.GroupId, topic.Key);
                    foreach (KeyValuePair<int, PartitionTopicInfo> partition in topic.Value)
                    {
                        var newOffset = partition.Value.ConsumeOffset + 1;
                        try
                        {
                            if (partition.Value.ConsumeOffsetValid)
                            {
                                // Save offsets unconditionally. Kafka's latestOffset for a particular topic-partition can go backward
                                // if a follwer which is not fully caught up becomes a leader. We still need to save the conumed offsets even then.
                                //skip only if we are trying to commit the same offset

                                if (newOffset != partition.Value.CommitedOffset)
                                {
                                    try
                                    {
                                        ZkUtils.UpdatePersistentPath(GetZkClient(),
                                                                        topicDirs.ConsumerOffsetDir + "/" +
                                                                        partition.Value.PartitionId, newOffset.ToString());
                                        partition.Value.CommitedOffset = newOffset;
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.ErrorFormat("error in CommitOffsets UpdatePersistentPath : {0}", ex.FormatException());
                                    }
                                }
                            }
                            else
                            {
                                Logger.InfoFormat("Skip committing offset {0} for topic {1} because it is invalid (ZK session is disconnected)", newOffset, partition);
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.WarnFormat("exception during CommitOffsets: {0}", ex.FormatException());
                        }

                        if (Logger.IsDebugEnabled)
                        {
                            Logger.DebugFormat("Commited offset {0} for topic {1}", newOffset, partition);
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("error in CommitOffsets : {0}", ex.FormatException());
            }
        }

        public void AutoCommit()
        {
            this.EnsuresNotDisposed();
            try
            {
                this.CommitOffsets();
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("exception during AutoCommit: {0}", ex.FormatException());
            }
        }

        /// <summary>
        /// Commit offset of specified topic/partition.
        /// Only used when customer has strong requirement for reprocess messages as few as possible.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partition"></param>
        /// <param name="offset"></param>
        /// <param name="setPosition">Indicates whether to set the fetcher's offset to the value committed. Default = true.</param>
        public void CommitOffset(string topic, int partition, long offset, bool setPosition = true)
        {
            this.EnsuresNotDisposed();
            if (this.GetZkClient() == null)
            {
                return;
            }
            if (this.config.AutoCommit == true)
            {
                throw new ArgumentException(string.Format("When do commit offset with desired partition and offset, must set AutoCommit of ConsumerConfiguration as false!"));
            }
            try
            {
                IDictionary<int, PartitionTopicInfo> topicPartitionInfo = topicRegistry[topic];
                var topicDirs = new ZKGroupTopicDirs(this.config.GroupId, topic);
                PartitionTopicInfo partitionTopicInfo = topicPartitionInfo[partition];
                if (partitionTopicInfo.ConsumeOffsetValid)
                {
                    //Commit offset unconditionally. This would give consumes to decide which offset to read/skip
                    //if (offset > partitionTopicInfo.CommitedOffset)
                    try
                    {
                        ZkUtils.UpdatePersistentPath(GetZkClient(),
                            topicDirs.ConsumerOffsetDir + "/" +
                            partitionTopicInfo.PartitionId, offset.ToString());
                        partitionTopicInfo.CommitedOffset = offset;
                        if (setPosition)
                        {
                            partitionTopicInfo.ConsumeOffset = offset;
                            partitionTopicInfo.FetchOffset = offset;
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.ErrorFormat("error in CommitOffsets UpdatePersistentPath : {0}", ex.FormatException());
                    }
                }
                else
                {
                    Logger.InfoFormat(
                        "Skip committing offset {0} for topic {1} because it is invalid (ZK session is disconnected)",
                        offset, partitionTopicInfo);
                }

                if (Logger.IsDebugEnabled)
                {
                    Logger.DebugFormat("Commited offset {0} for topic {1}", offset, partitionTopicInfo);
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("exception during CommitOffsets: Topic:{0}  Partition:{1} offset:{2} Exception:{3} ", topic, partition, offset, ex.FormatException());
            }
        }

        public string GetConsumerIdString()
        {
            return this.config.GroupId + "_" + this.config.ConsumerId;
        }

        public void ReleaseAllPartitionOwnerships()
        {
            Logger.Info("Releasing all partition ownerships");

            string consumerIdString = GetConsumerIdString();

            foreach (KeyValuePair<string, IDictionary<int, PartitionTopicInfo>> item in topicRegistry)
            {
                var topic = item.Key;
                try
                {
                    foreach (var partition in item.Value.Keys)
                    {
                        var partitionOwnerPath = ZkUtils.GetConsumerPartitionOwnerPath(this.config.GroupId, topic,
                                                                                      partition.ToString());
                        Logger.InfoFormat("Consumer {0} will delete ZK path {1} topic:{2} partition:{3} ", consumerIdString, partitionOwnerPath, topic, partition);
                        try
                        {
                            GetZkClient().SlimLock.EnterWriteLock();
                            ZkUtils.DeletePath(GetZkClient(), partitionOwnerPath);
                            Logger.InfoFormat("Consumer {0} SUCC delete ZK path {1} topic:{2} partition:{3}  succsessfully.", consumerIdString, partitionOwnerPath, topic, partition);
                        }
                        catch (Exception ex)
                        {
                            Logger.ErrorFormat("Consumer {0} FAILED delete ZK path {1} topic:{2} partition:{3}  error:{4}.", consumerIdString, partitionOwnerPath, topic, partition, ex.FormatException());
                        }
                        finally
                        {
                            GetZkClient().SlimLock.ExitWriteLock();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.ErrorFormat("error when call ZkUtils.DeletePath : {0}", ex.FormatException());
                }
            }

            Logger.Info("Released all partition ownerships");
        }

        /// <summary>
        /// Creates a list of message streams for each topic.
        /// </summary>
        /// <param name="topicCountDict">
        /// The map of topic on number of streams
        /// </param>
        /// <returns>
        /// The list of <see cref="KafkaMessageStream"/>, which are iterators over topic.
        /// </returns>
        /// <remarks>
        /// Explicitly triggers load balancing for this consumer
        /// </remarks>
        public IDictionary<string, IList<KafkaMessageStream<TData>>> CreateMessageStreams<TData>(IDictionary<string, int> topicCountDict, IDecoder<TData> decoder)
        {
            this.EnsuresNotDisposed();
            return this.Consume(topicCountDict, decoder);
        }

        IDictionary<string, IList<IKafkaMessageStream<TData>>> IZookeeperConsumerConnector.CreateMessageStreams<TData>(IDictionary<string, int> topicCountDict, IDecoder<TData> decoder)
        {
            return CreateMessageStreams(topicCountDict, decoder)
                .ToDictionary(
                    kvp => kvp.Key,
                    kvp => (IList<IKafkaMessageStream<TData>>)kvp.Value.Cast<IKafkaMessageStream<TData>>().ToList()
                );
        }

        public Dictionary<int, long> GetOffset(string topic)
        {
            Dictionary<int, long> offsets = new Dictionary<int, long>();
            this.EnsuresNotDisposed();
            if (this.GetZkClient() == null)
            {
                throw new ArgumentNullException(string.Format("zkClient {0} has not been initialized!", this.config.ZooKeeper.ZkConnect));
            }
            var topicDirs = new ZKGroupTopicDirs(this.config.GroupId, topic);
            if (!GetZkClient().Exists(topicDirs.ConsumerOffsetDir))
            {
                Logger.ErrorFormat("Path {0} not exists on zookeeper {1}, maybe the consumer group hasn't commit once yet. ", topicDirs.ConsumerOffsetDir, this.config.ZooKeeper.ZkConnect);
                return offsets;
            }

            IEnumerable<string> partitions = GetZkClient().GetChildren(topicDirs.ConsumerOffsetDir);
            foreach (var p in partitions)
            {
                string fullPatht = topicDirs.ConsumerOffsetDir + "/" + p;

                string data = GetZkClient().ReadData<string>(fullPatht, true);
                offsets.Add(Convert.ToInt32(p), Convert.ToInt64(data));
            }
            return offsets;
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (this.disposed)
            {
                return;
            }

            Logger.Info("ZookeeperConsumerConnector shutting down and dispose ...");

            try
            {
                // Stop any async rebalance operations that might be running
                stopAsyncRebalancing.ForEach(s => s.Invoke());
                if (UseSharedStaticZookeeperClient)
                {
                    Logger.InfoFormat("will call Unsubscribe since use static zkClient. subscribedChildCollection:{0} , subscribedZookeeperStateCollection:{1} subscribedZookeeperDataCollection:{2} "
                        , subscribedChildCollection.Count, subscribedZookeeperStateCollection.Count, subscribedZookeeperDataCollection.Count);
                    if (GetZkClient() != null)
                    {
                        foreach (Tuple<string, IZooKeeperChildListener> t in this.subscribedChildCollection)
                        {
                            GetZkClient().Unsubscribe(t.Item1, t.Item2);
                        }

                        foreach (IZooKeeperStateListener t in this.subscribedZookeeperStateCollection)
                        {
                            GetZkClient().Unsubscribe(t);
                        }

                        foreach (Tuple<string, IZooKeeperDataListener> t in this.subscribedZookeeperDataCollection)
                        {
                            GetZkClient().Unsubscribe(t.Item1, t.Item2);
                        }
                    }
                    else
                    {
                        Logger.Warn("STATIC zkCLient still null. ");
                    }

                    subscribedChildCollection = new ConcurrentBag<Tuple<string, IZooKeeperChildListener>>();
                    subscribedZookeeperStateCollection = new ConcurrentBag<IZooKeeperStateListener>();
                    subscribedZookeeperDataCollection = new ConcurrentBag<Tuple<string, IZooKeeperDataListener>>();
                    Logger.InfoFormat("Finish call Unsubscribe since use static zkClient. collection have been clean up. subscribedChildCollection:{0} , subscribedZookeeperStateCollection:{1} subscribedZookeeperDataCollection:{2} "
                      , subscribedChildCollection.Count, subscribedZookeeperStateCollection.Count, subscribedZookeeperDataCollection.Count);
                }
                else
                {
                    Logger.Info("will call UnsubscribeAll since use local zkClient. ");
                    this.GetZkClient().UnsubscribeAll();
                    Logger.Info("After call UnsubscribeAll since use local zkClient. ");
                }

                if (this.scheduler != null)
                {
                    this.scheduler.Dispose();
                }

                System.Threading.Thread.Sleep(4000);

                if (this.fetcher != null)
                {
                    this.fetcher.Dispose();
                }

                this.SendShutdownToAllQueues();
                if (this.config.AutoCommit)
                {
                    this.CommitOffsets();
                }

                lock (this.shuttingDownLock)
                {
                    if (this.disposed)
                    {
                        return;
                    }
                    this.disposed = true;
                }

                if (UseSharedStaticZookeeperClient)
                {
                    Logger.Info("will NOT call zkClient.Dispose() since using static one");
                    Logger.Info("will explicitly call ReleaseAllPartitionOwnerships() since using static one");
                    ReleaseAllPartitionOwnerships();
                    Logger.Info("will explicitly call DeleteConsumerIdNode() since using static one");
                    DeleteConsumerIdNode();
                }
                else
                {
                    Logger.Info("will call  this.zkClient.Dispose(); ");
                    if (this.GetZkClient() != null)
                    {
                        this.GetZkClient().Dispose();
                    }
                }
            }
            catch (Exception exc)
            {
                Logger.Warn("Ignoring unexpected errors on shutting down", exc);
            }

            Logger.Info("ZookeeperConsumerConnector shut down completed");
        }

        private void DeleteConsumerIdNode()
        {
            string consumerIdString = GetConsumerIdString();
            ZKGroupDirs dirs = new ZKGroupDirs(this.config.GroupId);

            string idsPath = dirs.ConsumerRegistryDir + "/" + consumerIdString;
            Logger.InfoFormat("Will delete  {0}  in zookeeper due to zookeeperConsumerConnector dispose.", idsPath);
            try
            {
                GetZkClient().SlimLock.EnterWriteLock();
                ZkUtils.DeletePath(GetZkClient(), idsPath);
                Logger.InfoFormat("Path {0} deleted succsessfully.", idsPath);
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Path {0} FAILED to be deleted: {1}", idsPath, ex.FormatException());
            }
            finally
            {
                GetZkClient().SlimLock.ExitWriteLock();
            }
        }

        private void ConnectZk()
        {
            Logger.Info("Enter connectZk()");
            if (UseSharedStaticZookeeperClient)
            {
                Logger.Info("After check  UseSharedStaticZookeeperClient");
                if (zkClientStatic == null || (zkClientStatic != null && zkClientStatic.GetClientState() != ZooKeeperNet.KeeperState.SyncConnected))
                {
                    Logger.Info("After check  UseSharedStaticZookeeperClient, will lock");
                    lock (zkClientStaticLock)
                    {
                        Logger.Info("got lock ... ");
                        if (zkClientStatic == null || (zkClientStatic != null && zkClientStatic.GetClientState() != ZooKeeperNet.KeeperState.SyncConnected))
                        {
                            Logger.InfoFormat("zkClientStatic: {0}   will create one ...", zkClientStatic == null ? "null" : "not null");
                            Logger.InfoFormat("Connecting to zookeeper instance at {0}  STATIC", this.config.ZooKeeper.ZkConnect);
                            zkClientStatic = new ZooKeeperClient(this.config.ZooKeeper.ZkConnect, this.config.ZooKeeper.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer, this.config.ZooKeeper.ZkConnectionTimeoutMs);
                            zkClientStatic.Connect();
                            Logger.InfoFormat("Connecting to zookeeper instance at {0}  STATIC. Done", this.config.ZooKeeper.ZkConnect);
                        }

                        Logger.Info("release lock ... ");
                    }
                }

                Logger.InfoFormat("zkClientStatic: {0}", zkClientStatic == null ? "null" : "not null");
                if (zkClientStatic != null)
                {
                    Logger.InfoFormat("zkClientStatic.ClientState: {0}", zkClientStatic.GetClientState());
                }
            }
            else
            {
                Logger.InfoFormat("Connecting to zookeeper instance at {0}", this.config.ZooKeeper.ZkConnect);
                if (this.zkClientInternal != null)
                {
                    this.zkClientInternal.Dispose();
                    this.zkClientInternal = null;
                }
                this.zkClientInternal = new ZooKeeperClient(this.config.ZooKeeper.ZkConnect, this.config.ZooKeeper.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer, this.config.ZooKeeper.ZkConnectionTimeoutMs);
                this.zkClientInternal.Connect();
            }
        }

        private void CreateFetcher()
        {
            if (this.enableFetcher)
            {
                this.fetcher = new Fetcher(this.config, this.GetZkClient());
            }
        }

        private IDictionary<string, IList<KafkaMessageStream<TData>>> Consume<TData>(IDictionary<string, int> topicCountDict, IDecoder<TData> decoder)
        {
            Logger.Debug("entering consume");

            if (topicCountDict == null)
            {
                throw new ArgumentNullException();
            }

            var dirs = new ZKGroupDirs(this.config.GroupId);
            var result = new Dictionary<string, IList<KafkaMessageStream<TData>>>();

            string consumerIdString = GetConsumerIdString();
            var topicCount = new TopicCount(consumerIdString, topicCountDict);

            //// create a queue per topic per consumer thread
            var consumerThreadIdsPerTopicMap = topicCount.GetConsumerThreadIdsPerTopic();
            foreach (var topic in consumerThreadIdsPerTopicMap.Keys)
            {
                var streamList = new List<KafkaMessageStream<TData>>();
                foreach (string threadId in consumerThreadIdsPerTopicMap[topic])
                {
                    var stream = new BlockingCollection<FetchedDataChunk>(new ConcurrentQueue<FetchedDataChunk>());
                    this.queues.Add(new Tuple<string, string>(topic, threadId), stream);
                    streamList.Add(new KafkaMessageStream<TData>(topic, stream, this.config.Timeout, decoder));
                }

                result.Add(topic, streamList);
                Logger.InfoFormat("adding topic {0} and stream to map...", topic);
            }

            // listener to consumer and partition changes
            var loadBalancerListener = new ZKRebalancerListener<TData>(
                this.config,
                consumerIdString,
                this.topicRegistry,
                this.GetZkClient(),
                this,
                queues,
                this.fetcher,
                result,
                topicCount);

            if (this.consumerRebalanceHandler != null)
            {
                loadBalancerListener.ConsumerRebalance += this.consumerRebalanceHandler;
            }

            stopAsyncRebalancing.Add(loadBalancerListener.StopRebalance);
            this.RegisterConsumerInZk(dirs, consumerIdString, topicCount);

            //// register listener for session expired event
            var zkSessionExpireListener = new ZKSessionExpireListener<TData>(dirs, consumerIdString, topicCount, loadBalancerListener, this);
            if (this.zkSessionDisconnectedHandler != null)
            {
                zkSessionExpireListener.ZKSessionDisconnected += this.zkSessionDisconnectedHandler;
            }

            if (this.zkSessionExpiredHandler != null)
            {
                zkSessionExpireListener.ZKSessionExpired += this.zkSessionExpiredHandler;
            }

            this.GetZkClient().Subscribe(zkSessionExpireListener);
            this.subscribedZookeeperStateCollection.Add(zkSessionExpireListener);

            this.GetZkClient().Subscribe(dirs.ConsumerRegistryDir, loadBalancerListener);
            this.subscribedChildCollection.Add(new Tuple<string, IZooKeeperChildListener>(dirs.ConsumerRegistryDir, loadBalancerListener));

            result.ForEach(topicAndStreams =>
            {
                // register on broker partition path changes
                string partitionPath = ZooKeeperClient.DefaultBrokerTopicsPath + "/" + topicAndStreams.Key;
                if (this.GetZkClient().Exists(partitionPath))
                {
                    this.GetZkClient().Subscribe(partitionPath, loadBalancerListener);
                    this.subscribedChildCollection.Add(new Tuple<string, IZooKeeperChildListener>(partitionPath, loadBalancerListener));
                    // Create a mapping of all topic partitions and their current leaders
                    var topicsAndPartitions = ZkUtils.GetPartitionsForTopics(this.GetZkClient(), new[] { topicAndStreams.Key });
                    Dictionary<string, int> partitionLeaderMap = new Dictionary<string, int>();
                    foreach (var partitionId in topicsAndPartitions[topicAndStreams.Key])
                    {
                        // Find/parse current partition leader for this partition and add it
                        // to the mapping object                    
                        var partitionStatePath = partitionPath + "/partitions/" + partitionId + "/state";
                        this.GetZkClient().MakeSurePersistentPathExists(partitionStatePath);
                        int? partitionLeader = ZkUtils.GetLeaderForPartition(this.GetZkClient(), topicAndStreams.Key, int.Parse(partitionId));
                        partitionLeaderMap.Add(partitionStatePath, partitionLeader.GetValueOrDefault(-1));
                    }

                    // listen for changes on the state nodes for the partitions
                    // this will indicate when a leader switches, or the in sync replicas change                 
                    var leaderListener = new ZkPartitionLeaderListener<TData>(loadBalancerListener, partitionLeaderMap);
                    foreach (var partitionId in topicsAndPartitions[topicAndStreams.Key])
                    {
                        var partitionStatePath = partitionPath + "/partitions/" + partitionId + "/state";
                        this.GetZkClient().Subscribe(partitionStatePath, leaderListener);
                        this.subscribedZookeeperDataCollection.Add(new Tuple<string, IZooKeeperDataListener>(partitionStatePath, leaderListener));
                    }
                }
                else
                {
                    Logger.WarnFormat("The topic path at {0}, does not exist.", partitionPath);
                }
            });

            //// explicitly trigger load balancing for this consumer
            Logger.Info("Performing rebalancing. A new consumer has been added to consumer group: " + dirs.ConsumerRegistryDir + ", consumer: " + consumerIdString);
            Logger.InfoFormat("Subscribe count: subscribedChildCollection:{0} , subscribedZookeeperStateCollection:{1} subscribedZookeeperDataCollection:{2} "
                      , subscribedChildCollection.Count, subscribedZookeeperStateCollection.Count, subscribedZookeeperDataCollection.Count);

            //// When a new consumer join, need wait for rebalance finish to make sure Fetcher thread started.
            loadBalancerListener.AsyncRebalance(DefaultWaitTimeForInitialRebalanceInSeconds * 1000);

            return result;
        }

        private void SendShutdownToAllQueues()
        {
            foreach (var queue in this.queues)
            {
                Logger.InfoFormat("Clearing up queue");
                // clear the queue
                while (queue.Value.Count > 0)
                {
                    FetchedDataChunk item = null;
                    queue.Value.TryTake(out item);
                }

                queue.Value.Add(ShutdownCommand);
                Logger.InfoFormat("Cleared queue and sent shutdown command");
            }
        }

        internal void RegisterConsumerInZk(ZKGroupDirs dirs, string consumerIdString, TopicCount topicCount)
        {
            this.EnsuresNotDisposed();
            Logger.InfoFormat("begin registering consumer {0} in ZK", consumerIdString);
            try
            {
                this.GetZkClient().SlimLock.EnterWriteLock();
                ZkUtils.CreateEphemeralPathExpectConflict(this.GetZkClient(), dirs.ConsumerRegistryDir + "/" + consumerIdString, topicCount.ToJsonString());
                Logger.InfoFormat("successfully registering consumer {0} in ZK", consumerIdString);
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("error in RegisterConsumerInZk CreateEphemeralPathExpectConflict : {0}", ex.FormatException());
            }
            finally
            {
                GetZkClient().SlimLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }

        private IZooKeeperClient GetZkClient()
        {
            if (UseSharedStaticZookeeperClient)
                return zkClientStatic;
            else
                return this.zkClientInternal;
        }
    }
}
