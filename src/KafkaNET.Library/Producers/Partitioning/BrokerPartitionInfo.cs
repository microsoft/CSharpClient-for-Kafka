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



namespace Kafka.Client.Producers.Partitioning
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public class BrokerPartitionInfo : IBrokerPartitionInfo
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(BrokerPartitionInfo));

        private readonly ISyncProducerPool syncProducerPool;
        private readonly IDictionary<string, TopicMetadata> topicPartitionInfo = new Dictionary<string, TopicMetadata>();
        private readonly Dictionary<string, List<Partition>> topicPartitionInfoList = new Dictionary<string, List<Partition>>();
        private readonly Dictionary<string, Dictionary<int, int[]>> topicDataInZookeeper = new Dictionary<string, Dictionary<int, int[]>>();

        private readonly IDictionary<string, DateTime> topicPartitionInfoLastUpdateTime = new Dictionary<string, DateTime>();
        private readonly object updateLock = new object();
        private int topicMetaDataRefreshIntervalMS;
        private readonly ZooKeeperClient zkClient;

        public BrokerPartitionInfo(ISyncProducerPool syncProducerPool, IDictionary<string, TopicMetadata> cache, IDictionary<string, DateTime> lastUpdateTime, int topicMetaDataRefreshIntervalMS, ZooKeeperClient zkClient)
        {
            this.syncProducerPool = syncProducerPool;
            this.topicPartitionInfo = cache;
            this.topicPartitionInfoLastUpdateTime = lastUpdateTime;
            this.topicMetaDataRefreshIntervalMS = topicMetaDataRefreshIntervalMS;
            this.zkClient = zkClient;
        }

        public BrokerPartitionInfo(ISyncProducerPool syncProducerPool)
        {
            this.syncProducerPool = syncProducerPool;
        }

        /// <summary>
        /// Return leader of each partition.
        /// </summary>
        /// <param name="versionId"></param>
        /// <param name="clientId"></param>
        /// <param name="correlationId"></param>
        /// <param name="topic"></param>
        /// <returns></returns>
        public List<Partition> GetBrokerPartitionInfo(short versionId, string clientId, int correlationId, string topic)
        {
            UpdateInfoInternal(versionId, correlationId, clientId, topic);
            return GetBrokerPartitionInfo(topic);
        }

        public List<Partition> GetBrokerPartitionInfo(string topic)
        {
            if (!this.topicPartitionInfoList.ContainsKey(topic))
            {
                throw new KafkaException(string.Format("There is no  metadata  for topic {0} ", topic));
            }

            var metadata = this.topicPartitionInfo[topic];
            if (metadata.Error != ErrorMapping.NoError)
            {
                throw new KafkaException(string.Format("The metadata status for topic {0} is abnormal, detail: ", topic), metadata.Error);
            }

            return this.topicPartitionInfoList[topic];
        }
        public IDictionary<int, Broker> GetBrokerPartitionLeaders(short versionId, string clientId, int correlationId, string topic)
        {
            UpdateInfoInternal(versionId, correlationId, clientId, topic);
            return GetBrokerPartitionLeaders(topic);
        }

        public IDictionary<int, Broker> GetBrokerPartitionLeaders(string topic)
        {
            TopicMetadata metadata = this.topicPartitionInfo[topic];
            if (metadata.Error != ErrorMapping.NoError)
            {
                throw new KafkaException(string.Format("The metadata status for topic {0} is abnormal, detail: ", topic), metadata.Error);
            }

            Dictionary<int, Broker> partitionLeaders = new Dictionary<int, Broker>();
            foreach (var p in metadata.PartitionsMetadata)
            {
                if (p.Leader != null && !partitionLeaders.ContainsKey(p.PartitionId))
                {
                    partitionLeaders.Add(p.PartitionId, p.Leader);
                    Logger.DebugFormat("Topic {0} partition {1} has leader {2}", topic,
                                       p.PartitionId, p.Leader.Id);
                }

                if (p.Leader == null)
                    Logger.DebugFormat("Topic {0} partition {1} does not have a leader yet", topic,
                                                p.PartitionId);
            }
            return partitionLeaders;
        }

        /// <summary>
        /// Force get topic metadata and update 
        /// </summary>
        public void UpdateInfo(short versionId, int correlationId, string clientId, string topic)
        {
            Logger.InfoFormat("Will update metadata for topic:{0}", topic);
            Guard.NotNullNorEmpty(topic, "topic");
            var shuffledBrokers = this.syncProducerPool.GetShuffledProducers();
            var i = 0;
            var hasFetchedInfo = false;
            while (i < shuffledBrokers.Count && !hasFetchedInfo)
            {
                ISyncProducer producer = shuffledBrokers[i++];

                try
                {
                    var topicMetadataRequest = TopicMetadataRequest.Create(new List<string>() { topic }, versionId,
                                                                           correlationId, clientId);
                    var topicMetadataList = producer.Send(topicMetadataRequest);
                    var topicMetadata = topicMetadataList.Any() ? topicMetadataList.First() : null;
                    if (topicMetadata != null)
                    {
                        if (topicMetadata.Error != ErrorMapping.NoError)
                        {
                            Logger.WarnFormat("Try get metadata of topic {0} from {1}({2}) . Got error: {3}", topic, producer.Config.BrokerId, producer.Config.Host, topicMetadata.Error.ToString());
                        }
                        else
                        {
                            this.topicPartitionInfo[topic] = topicMetadata;
                            this.topicPartitionInfoLastUpdateTime[topic] = DateTime.UtcNow;
                            Logger.InfoFormat("Will  Update  metadata info, topic {0} ", topic);

                            //TODO:  For all partitions which has metadata, here return the sorted list.
                            //But sometimes kafka didn't return metadata for all topics.
                            this.topicPartitionInfoList[topic] = topicMetadata.PartitionsMetadata.Select(m =>
                                                        {
                                                            Partition partition = new Partition(topic, m.PartitionId);
                                                            if (m.Leader != null)
                                                            {
                                                                var leaderReplica = new Replica(m.Leader.Id, topic);
                                                                partition.Leader = leaderReplica;
                                                                Logger.InfoFormat("Topic {0} partition {1} has leader {2}", topic,
                                                                                   m.PartitionId, m.Leader.Id);

                                                                return partition;
                                                            }

                                                            Logger.WarnFormat("Topic {0} partition {1} does not have a leader yet", topic,
                                                                m.PartitionId);

                                                            return partition;
                                                        }
                                                        ).OrderBy(x => x.PartId).ToList(); ;
                            hasFetchedInfo = true;
                            Logger.InfoFormat("Finish  Update  metadata info, topic {0}  Partitions:{1}  No leader:{2}", topic, this.topicPartitionInfoList[topic].Count, this.topicPartitionInfoList[topic].Where(r => r.Leader == null).Count());

                            //In very weired case, the kafka broker didn't return metadata of all broker. need break and retry.  https://issues.apache.org/jira/browse/KAFKA-1998
                            // http://qnalist.com/questions/5899394/topicmetadata-response-miss-some-partitions-information-sometimes
                            if (zkClient != null)
                            {
                                Dictionary<int, int[]> topicMetaDataInZookeeper = ZkUtils.GetTopicMetadataInzookeeper(this.zkClient, topic);
                                if (topicMetaDataInZookeeper != null && topicMetaDataInZookeeper.Any())
                                {
                                    topicDataInZookeeper[topic] = topicMetaDataInZookeeper;
                                    if (this.topicPartitionInfoList[topic].Count != topicMetaDataInZookeeper.Count)
                                    {
                                        Logger.ErrorFormat("NOT all partition has metadata.  Topic partition in zookeeper :{0} topics has partition metadata: {1}", topicMetaDataInZookeeper.Count, this.topicPartitionInfoList[topic].Count);
                                        throw new UnavailableProducerException(string.Format("Please make sure every partition at least has one broker running and retry again.   NOT all partition has metadata.  Topic partition in zookeeper :{0} topics has partition metadata: {1}", topicMetaDataInZookeeper.Count, this.topicPartitionInfoList[topic].Count));
                                    }
                                }
                            }

                        }
                    }
                }
                catch (Exception e)
                {
                    Logger.ErrorFormat("Try get metadata of topic {0} from {1}({2}) . Got error: {3}", topic, producer.Config.BrokerId, producer.Config.Host, e.FormatException());
                }
            }
        }

        private void UpdateInfoInternal(short versionId, int correlationId, string clientId, string topic)
        {
            Logger.DebugFormat("Will try check if need update. broker partition info for topic {0}", topic);
            //check if the cache has metadata for this topic
            bool needUpdateForNotExists = false;
            bool needUpdateForExpire = false;
            if (!this.topicPartitionInfo.ContainsKey(topic) || this.topicPartitionInfo[topic].Error != ErrorMapping.NoError)
            {
                needUpdateForNotExists = true;
            }

            if (this.topicPartitionInfoLastUpdateTime.ContainsKey(topic)
                && (DateTime.UtcNow - this.topicPartitionInfoLastUpdateTime[topic]).TotalMilliseconds > this.topicMetaDataRefreshIntervalMS)
            {
                needUpdateForExpire = true;
                Logger.InfoFormat("Will update metadata for topic:{0}  Last update time:{1}  Diff:{2} Config:{3} ", topic, this.topicPartitionInfoLastUpdateTime[topic]
                    , (DateTime.UtcNow - this.topicPartitionInfoLastUpdateTime[topic]).TotalMilliseconds, this.topicMetaDataRefreshIntervalMS);
            }

            if (needUpdateForNotExists || needUpdateForExpire)
            {
                Logger.InfoFormat("Will update metadata for topic:{0} since: needUpdateForNotExists: {1}  needUpdateForExpire:{2} ", topic, needUpdateForNotExists, needUpdateForExpire);
                lock (updateLock)
                {
                    this.UpdateInfo(versionId, correlationId, clientId, topic);
                    if (!this.topicPartitionInfo.ContainsKey(topic))
                    {
                        throw new KafkaException(string.Format("Failed to fetch topic metadata for topic: {0} ", topic));
                    }
                }
            }
        }
    }
}
