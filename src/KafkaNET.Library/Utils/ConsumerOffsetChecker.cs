// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Utils
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Requests;
    using Kafka.Client.ZooKeeperIntegration;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using ZooKeeperNet;

    /// <summary>
    /// Helper class to collect statistics about Consumer's offsets, lags, etc.
    /// </summary>
    public class ConsumerOffsetChecker : KafkaClientBase
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumerOffsetChecker));

        private readonly IDictionary<int, Consumer> consumerDict = new Dictionary<int, Consumer>();
        private readonly ConsumerConfiguration config;
        private IZooKeeperClient zkClient;
        private Cluster kafkaCluster;
        private volatile bool disposed;
        private readonly object shuttingDownLock = new object();

        public ConsumerOffsetChecker(ConsumerConfiguration config)
        {
            this.config = config;
            this.ConnectZk();
        }

        /// <summary>
        /// Get the statistics about how many messages left in queue unconsumed by specified consumer group
        /// </summary>
        /// <param name="topics">Topics name that Consumer Group started consuming. If topics are null <see cref="Kafka.Client.Utils.ConsumerOffsetChecker"/> will automatically retrieve all topics for specified Consumer Group</param>
        /// <param name="consumerGroup">Consumer group name</param>
        /// <returns><see cref="Kafka.Client.Consumers.ConsumerGroupStatisticsRecord"/> that contains statistics of consuming specified topics by specified consumer group. If topics are null <see cref="Kafka.Client.Utils.ConsumerOffsetChecker"/> will automatically retrieve all topics for specified Consumer Group</returns>
        /// <remarks>If topics are null <see cref="Kafka.Client.Utils.ConsumerOffsetChecker"/> will automatically retrieve all topics for specified Consumer Group</remarks>
        public ConsumerGroupStatisticsRecord GetConsumerStatistics(ICollection<string> topics, string consumerGroup)
        {
            // retrive latest brokers info
            this.RefreshKafkaBrokersInfo();

            if (topics == null)
            {
                IEnumerable<string> zkTopics;
                try
                {
                    zkTopics = zkClient.GetChildren((new ZKGroupDirs(consumerGroup).ConsumerGroupDir + "/offsets"));
                }
                catch (KeeperException e)
                {
                    if (e.ErrorCode == KeeperException.Code.NONODE)
                        zkTopics = null;
                    else
                        throw;
                }

                if (zkTopics == null)
                {
                    throw new ArgumentException(
                        String.Format(CultureInfo.InvariantCulture, "Can't automatically retrieve topic list because consumer group {0} does not have any topics with commited offsets", consumerGroup));
                }

                topics = zkTopics.ToList();
            }

            Logger.InfoFormat("Collecting consumer offset statistics for consumer group {0}", consumerGroup);

            var consumerGroupState = new ConsumerGroupStatisticsRecord
            {
                ConsumerGroupName = consumerGroup,
                TopicsStat = new Dictionary<string, TopicStatisticsRecord>(topics.Count)
            };
            try
            {
                foreach (var topic in topics)
                {
                    Logger.DebugFormat("Collecting consumer offset statistics for consumer group {0}, topic {1}", consumerGroup, topic);
                    consumerGroupState.TopicsStat[topic] = this.ProcessTopic(consumerGroup, topic);
                }
            }
            catch (NoPartitionsForTopicException exc)
            {
                Logger.ErrorFormat("Could not find any partitions for topic {0}. Error: {1}", exc.Topic, exc.FormatException());
                throw;
            }
            catch (Exception exc)
            {
                Logger.ErrorFormat("Failed to collect consumer offset statistics for consumer group {0}. Error: {1}", consumerGroup, exc.FormatException());
                throw;
            }
            finally
            {
                // close created consumers
                foreach (var consumer in this.consumerDict)
                {
                    consumer.Value.Dispose();
                }

                consumerDict.Clear();
            }

            Logger.InfoFormat("Collecting consumer offset statistics for consumer group {0} completed", consumerGroup);

            return consumerGroupState;
        }

        private void RefreshKafkaBrokersInfo()
        {
            this.kafkaCluster = new Cluster(this.zkClient);
        }

        private TopicStatisticsRecord ProcessTopic(string consumerGroup, string topic)
        {
            var topicPartitionMap = ZkUtils.GetPartitionsForTopics(this.zkClient, new[] { topic });

            var topicState = new TopicStatisticsRecord
            {
                Topic = topic,
                PartitionsStat = new Dictionary<int, PartitionStatisticsRecord>(topicPartitionMap[topic].Count)
            };

            foreach (var partitionId in topicPartitionMap[topic])
            {
                try
                {
                    var partitionIdInt = int.Parse(partitionId);
                    topicState.PartitionsStat[partitionIdInt] = this.ProcessPartition(consumerGroup, topic, partitionIdInt);
                }
                catch (NoLeaderForPartitionException exc)
                {
                    Logger.ErrorFormat("Could not found a leader for partition {0}. Details: {1}", exc.PartitionId, exc.FormatException());
                }
                catch (BrokerNotAvailableException exc)
                {
                    Logger.ErrorFormat("Could not found a broker information for broker {0} while processing partition {1}. Details: {2}", exc.BrokerId, partitionId, exc.FormatException());
                }
                catch (OffsetIsUnknowException exc)
                {
                    Logger.ErrorFormat("Could not retrieve offset from broker {0} for topic {1} partition {2}. Details: {3}", exc.BrokerId, exc.Topic, exc.PartitionId, exc.FormatException());
                }
            }

            return topicState;
        }

        private PartitionStatisticsRecord ProcessPartition(string consumerGroup, string topic, int partitionId)
        {
            Logger.DebugFormat("Collecting consumer offset statistics for consumer group {0}, topic {1}, partition {2}", consumerGroup, topic, partitionId);

            // find partition leader and create a consumer instance
            var leader = ZkUtils.GetLeaderForPartition(this.zkClient, topic, partitionId);
            if (!leader.HasValue)
            {
                throw new NoLeaderForPartitionException(partitionId);
            }

            var consumer = this.GetConsumer(leader.Value);

            // get current offset
            long? currentOffset;
            var partitionIdStr = partitionId.ToString(CultureInfo.InvariantCulture);
            var znode = ZkUtils.GetConsumerPartitionOffsetPath(consumerGroup, topic, partitionIdStr);
            var currentOffsetString = this.zkClient.ReadData<string>(znode, true);
            if (currentOffsetString == null)
            {
                // if offset is not stored in ZooKeeper retrieve first offset from actual Broker
                currentOffset = ConsumerUtils.EarliestOrLatestOffset(consumer, topic, partitionId, OffsetRequest.EarliestTime);
                if (!currentOffset.HasValue)
                {
                    throw new OffsetIsUnknowException(topic, leader.Value, partitionId);
                }
            }
            else
            {
                currentOffset = long.Parse(currentOffsetString);
            }

            // get last offset
            var lastOffset = ConsumerUtils.EarliestOrLatestOffset(consumer, topic, partitionId, OffsetRequest.LatestTime);
            if (!lastOffset.HasValue)
            {
                throw new OffsetIsUnknowException(topic, leader.Value, partitionId);
            }

            var owner = this.zkClient.ReadData<string>(ZkUtils.GetConsumerPartitionOwnerPath(consumerGroup, topic, partitionIdStr), true);

            return new PartitionStatisticsRecord
            {
                PartitionId = partitionId,
                CurrentOffset = currentOffset.Value,
                LastOffset = lastOffset.Value,
                OwnerConsumerId = owner
            };

        }

        /// <summary>
        /// Get consumer instance for specified broker from cache. Or create a consumer if it does not already exist.
        /// </summary>
        /// <param name="brokerId">Broker id</param>
        /// <returns>Consumer instance for specified broker.</returns>
        private Consumer GetConsumer(int brokerId)
        {
            Consumer result;
            if (!this.consumerDict.TryGetValue(brokerId, out result))
            {
                var broker = this.kafkaCluster.GetBroker(brokerId);
                if (broker == null)
                {
                    throw new BrokerNotAvailableException(brokerId);
                }

                result = new Consumer(this.config, broker.Host, broker.Port);
                this.consumerDict.Add(brokerId, result);
            }

            return result;
        }

        private void ConnectZk()
        {
            Logger.InfoFormat("Connecting to zookeeper instance at {0}", this.config.ZooKeeper.ZkConnect);
            this.zkClient = new ZooKeeperClient(this.config.ZooKeeper.ZkConnect, this.config.ZooKeeper.ZkSessionTimeoutMs, ZooKeeperStringSerializer.Serializer, this.config.ZooKeeper.ZkConnectionTimeoutMs);
            this.zkClient.Connect();
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

            Logger.Info("ConsumerOffsetChecker shutting down");

            try
            {
                this.zkClient.UnsubscribeAll();

                System.Threading.Thread.Sleep(4000);

                lock (this.shuttingDownLock)
                {
                    if (this.disposed)
                    {
                        return;
                    }

                    this.disposed = true;
                }

                if (this.zkClient != null)
                {
                    this.zkClient.Dispose();
                }
            }
            catch (Exception exc)
            {
                Logger.Debug("Ignoring unexpected errors on shutting down", exc);
            }

            Logger.Info("ConsumerOffsetChecker shut down completed");
        }
    }
}
