// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Helper
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Manage :
    ///     syncProducerPool to get metadta/offset
    ///     ProducerPool to send.
    ///     ConsumerPool to consume.
    ///     
    ///     new KafkaSimpleManager()
    ///     Scenario 1:  
    ///         RefreshMetadata()
    ///         RefreshAndGetOffsetByTimeStamp()
    /// 
    /// 
    ///     Scenario 2 Produce :
    ///     
    ///         Sub scenario 1:  Send to random partition
    ///         Sub scenario 2:  Send to specific partition
    ///         For Sub scenario 1 and 2:
    ///             InitializeProducerPoolForTopic        
    ///             call GetProducer() to get required producer
    ///                 Send 
    ///                 If send failed, call  RefreshMetadataAndRecreateProducerWithPartition
    ///         
    /// 
    ///         Sub scenario 3:  Send to partition by default partitioner class.   Math.Abs(key.GetHashCode()) % numPartitions
    ///         Sub scenario 4:  Send to partition by customized partitioner
    ///         For Sub scenario 3 and 4:
    ///             InitializeProducerPoolForTopic
    ///             call GetProducerWithPartionerClass() to get producer
    ///                 Send
    ///                 If send failed, call RefreshMetadata and retry.
    ///
    ///     Scenario 3 Consume pool:
    ///         call GetConsumerFromPool() to get consumer
    ///             consume
    ///             If consume failed, call  GetConsumerFromPoolAfterRecreate() to get new consumer.
    ///         
    /// 
    /// </summary>
    public class KafkaSimpleManager<TKey, TData> : IDisposable
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger("KafkaSimpleManager");

        public KafkaSimpleManagerConfiguration Config { get; private set; }

        private ConcurrentDictionary<string, object> TopicLockProduce = new ConcurrentDictionary<string, object>();
        private ConcurrentDictionary<string, object> TopicPartitionLockConsume = new ConcurrentDictionary<string, object>();
        //topic -->  TopicMetadata
        private ConcurrentDictionary<string, TopicMetadata> TopicMetadatas = new ConcurrentDictionary<string, TopicMetadata>();
        //topic -->  TopicMetadatasLastUpdateTime
        private ConcurrentDictionary<string, DateTime> TopicMetadatasLastUpdateTime = new ConcurrentDictionary<string, DateTime>();
        //topic --> partitionid -->
        private ConcurrentDictionary<string, Dictionary<int, Tuple<Broker, BrokerConfiguration>>> TopicMetadataPartitionsLeaders = new ConcurrentDictionary<string, Dictionary<int, Tuple<Broker, BrokerConfiguration>>>();
        private ConcurrentDictionary<string, ConcurrentDictionary<int, long>> TopicOffsetEarliest = new ConcurrentDictionary<string, ConcurrentDictionary<int, long>>();
        private ConcurrentDictionary<string, ConcurrentDictionary<int, long>> TopicOffsetLatest = new ConcurrentDictionary<string, ConcurrentDictionary<int, long>>();
        private object topicProducerLock = new object();
        //topic --> partitionid -->
        private ConcurrentDictionary<string, ConcurrentDictionary<int, Producer<TKey, TData>>> TopicPartitionsLeaderProducers = new ConcurrentDictionary<string, ConcurrentDictionary<int, Producer<TKey, TData>>>();
        private ConcurrentDictionary<string, Producer<TKey, TData>> TopicProducersWithPartitionerClass = new ConcurrentDictionary<string, Producer<TKey, TData>>();
        //topic --> partitionid -->
        private ConcurrentDictionary<string, ConcurrentDictionary<int, Consumer>> TopicPartitionsLeaderConsumers = new ConcurrentDictionary<string, ConcurrentDictionary<int, Consumer>>();

        #region SyncProducerPool for metadata.
        private volatile bool disposed = false;
        // the pool of syncProducer for TopicMetaData requests, which retrieve PartitionMetaData, including leaders and ISRs.
        private volatile SyncProducerPool syncProducerPoolForMetaData = null;
        private object syncProducerPoolForMetadataLock = new object();
        private Random random = new Random();
        private Random randomForGetCachedProducer = new Random();
        #endregion

        public KafkaSimpleManager(KafkaSimpleManagerConfiguration config)
        {
            this.Config = config;
            this.RecreateSyncProducerPoolForMetadata();
        }

        #region SyncProducer Pool  ForMetadata refresh
        /// <summary>
        /// Initialize SyncProducerPool used for get metadata by query Zookeeper
        /// Here only build connection for all ACTIVE broker.  So client actually need regularly dispose/recreate or refresh, for example, every 20 minutes..
        /// </summary>
        public void RecreateSyncProducerPoolForMetadata()
        {
            SyncProducerPool tempSyncProducerPool = null;
            if (this.Config.ZookeeperConfig != null)
            {
                // Honor Zookeeper connection string only when KafkaBroker list not provided
                var producerConfig = new ProducerConfiguration(new List<BrokerConfiguration>());
                producerConfig.ZooKeeper = this.Config.ZookeeperConfig;
                //This pool only for get metadata, so set SyncProducerOfOneBroker to 1 is enough.
                producerConfig.SyncProducerOfOneBroker = 1;
                tempSyncProducerPool = new SyncProducerPool(producerConfig);
            }

            if (this.syncProducerPoolForMetaData != null)
            {
                lock (syncProducerPoolForMetadataLock)
                {
                    this.syncProducerPoolForMetaData.Dispose();
                    this.syncProducerPoolForMetaData = null;
                    this.syncProducerPoolForMetaData = tempSyncProducerPool;
                }
            }
            else
                this.syncProducerPoolForMetaData = tempSyncProducerPool;

            if (this.syncProducerPoolForMetaData == null || this.syncProducerPoolForMetaData.Count() == 0)
            {
                string s = string.Format("KafkaSimpleManager[{0}] SyncProducerPool Initialization produced empty syncProducer list, please check path /brokers/ids in zookeeper={1} to make sure ther is active brokers.",
                    this.GetHashCode().ToString("X"), this.Config.ZookeeperConfig);
                Logger.Error(s);
                this.syncProducerPoolForMetaData = null;
                throw new ArgumentException(s);
            }
            else
            {
                Logger.InfoFormat("The syncProducerPoolForMetaData:{0}", this.syncProducerPoolForMetaData.Count());
                foreach (KeyValuePair<int, SyncProducerPool.SyncProducerWrapper> kv in this.syncProducerPoolForMetaData.syncProducers)
                {
                    Logger.InfoFormat("\tBrokerID:  {0}  count: {1}", kv.Key, kv.Value.Producers.Count);
                }
            }

            Logger.InfoFormat("KafkaSimpleManager[{0}] SyncProducerPool initialized", this.GetHashCode().ToString("X"));
        }

        public List<string> GetTopicPartitionsFromZK(string topic)
        {
            return this.syncProducerPoolForMetaData.zkClient.GetChildren(string.Format("/brokers/topics/{0}/partitions", topic)).ToList();
        }

        /// <summary>
        /// Reset syncProducer pool
        /// </summary>
        private void ClearSyncProducerPoolForMetadata()
        {
            if (this.syncProducerPoolForMetaData != null)
            {
                this.syncProducerPoolForMetaData.Dispose();
                this.syncProducerPoolForMetaData = null;
            }
            Logger.DebugFormat("KafkaSyncProducerPoolManager[{0}] SyncProducerPool cleared", this.GetHashCode().ToString("X"));
        }
        #endregion

        #region Metadata, leader, configuration
        /// <summary>
        /// Refresh metadata of one topic and return.
        /// If can't get metadata for specified topic at first time, will RecreateSyncProducerPoolForMetadata and retry once.
        /// MANIFOLD use.
        /// </summary>
        public TopicMetadata RefreshMetadata(short versionId, string clientId, int correlationId, string topic, bool force)
        {
            Logger.InfoFormat("RefreshMetadata enter: {0} {1} {2} Topic:{3} Force:{4}", versionId, clientId, correlationId, topic, force);
            if (!force && this.TopicMetadatas.ContainsKey(topic))
                return this.TopicMetadatas[topic];

            int retry = 0;
            while (retry < 2)
            {
                Dictionary<string, TopicMetadata> tempTopicMetadatas = new Dictionary<string, TopicMetadata>();
                Dictionary<string, DateTime> tempTopicMetadatasLastUpdateTime = new Dictionary<string, DateTime>();
                Dictionary<int, Tuple<Broker, BrokerConfiguration>> partitionLeaders = new Dictionary<int, Tuple<Broker, BrokerConfiguration>>();
                RefreshMetadataInternal(versionId, clientId, correlationId, topic, tempTopicMetadatas, tempTopicMetadatasLastUpdateTime, partitionLeaders);

                if (tempTopicMetadatas.ContainsKey(topic))
                {
                    this.TopicMetadatas[topic] = tempTopicMetadatas[topic];
                    this.TopicMetadatasLastUpdateTime[topic] = tempTopicMetadatasLastUpdateTime[topic];
                    this.TopicMetadataPartitionsLeaders[topic] = partitionLeaders;
                    int partitionCountInZK = GetTopicPartitionsFromZK(topic).Count;
                    if (partitionCountInZK != partitionLeaders.Count)
                        Logger.WarnFormat("RefreshMetadata exit return. Some partitions has no leader.  Topic:{0}  PartitionMetadata:{1} partitionLeaders:{2} != partitionCountInZK:{3}", topic, tempTopicMetadatas[topic].PartitionsMetadata.Count(), partitionLeaders.Count, partitionCountInZK);
                    else
                        Logger.InfoFormat("RefreshMetadata exit return. Topic:{0}  PartitionMetadata:{1} partitionLeaders:{2} partitionCountInZK:{3}", topic, tempTopicMetadatas[topic].PartitionsMetadata.Count(), partitionLeaders.Count, partitionCountInZK);
                    return this.TopicMetadatas[topic];
                }
                else
                {
                    Logger.WarnFormat("Got null for metadata of topic {0}, will RecreateSyncProducerPoolForMetadata and retry . ", topic);
                    RecreateSyncProducerPoolForMetadata();
                }
                retry++;
            }

            Logger.WarnFormat("RefreshMetadata exit return NULL: {0} {1} {2} Topic:{3} Force:{4}", versionId, clientId, correlationId, topic, force);
            return null;
        }

        public TopicMetadata GetTopicMetadta(string topic)
        {
            return this.TopicMetadatas[topic];
        }

        /// <summary>
        /// Get leader broker of one topic partition. without retry.
        /// If got exception from this, probably need client code call RefreshMetadata with force = true.
        /// </summary>
        internal BrokerConfiguration GetLeaderBrokerOfPartition(string topic, int partitionID)
        {
            if (!this.TopicMetadatas.ContainsKey(topic))
            {
                throw new KafkaException(string.Format("There is no  metadata  for topic {0}.  Please call RefreshMetadata with force = true and try again.", topic));
            }

            TopicMetadata metadata = this.TopicMetadatas[topic];
            if (metadata.Error != ErrorMapping.NoError)
            {
                throw new KafkaException(string.Format("The metadata status for topic {0} is abnormal, detail: ", topic), metadata.Error);
            }

            if (!this.TopicMetadataPartitionsLeaders[topic].ContainsKey(partitionID))
                throw new NoLeaderForPartitionException(string.Format("No leader for topic {0} parition {1} ", topic, partitionID));

            return this.TopicMetadataPartitionsLeaders[topic][partitionID].Item2;
        }

        private void RefreshMetadataInternal(short versionId, string clientId, int correlationId, string topic, Dictionary<string, TopicMetadata> tempTopicMetadatas, Dictionary<string, DateTime> tempTopicMetadatasLastUpdateTime, Dictionary<int, Tuple<Broker, BrokerConfiguration>> partitionLeaders)
        {
            Logger.InfoFormat("RefreshMetadataInternal enter: {0} {1} {2} Topic:{3} ", versionId, clientId, correlationId, topic);

            lock (syncProducerPoolForMetadataLock)
            {
                BrokerPartitionInfo brokerPartitionInfo = new BrokerPartitionInfo(this.syncProducerPoolForMetaData, tempTopicMetadatas, tempTopicMetadatasLastUpdateTime, ProducerConfiguration.DefaultTopicMetaDataRefreshIntervalMS, this.syncProducerPoolForMetaData.zkClient);
                brokerPartitionInfo.UpdateInfo(versionId, correlationId, clientId, topic);
            }
            if (!tempTopicMetadatas.ContainsKey(topic))
            {
                throw new NoBrokerForTopicException(string.Format("There is no metadata for topic {0}.  Please check if all brokers of that topic live.", topic));
            }
            TopicMetadata metadata = tempTopicMetadatas[topic];
            if (metadata.Error != ErrorMapping.NoError)
            {
                throw new KafkaException(string.Format("The metadata status for topic {0} is abnormal, detail: ", topic), metadata.Error); ;
            }

            foreach (var p in metadata.PartitionsMetadata)
            {
                if (p.Leader != null && !partitionLeaders.ContainsKey(p.PartitionId))
                {
                    partitionLeaders.Add(p.PartitionId, new Tuple<Broker, BrokerConfiguration>(
                        p.Leader,
                        new BrokerConfiguration()
                        {
                            BrokerId = p.Leader.Id,
                            Host = p.Leader.Host,
                            Port = p.Leader.Port
                        }));
                    Logger.DebugFormat("RefreshMetadataInternal Topic {0} partition {1} has leader {2}", topic, p.PartitionId, p.Leader.Id);
                }
                if (p.Leader == null)
                    Logger.ErrorFormat("RefreshMetadataInternal Topic {0} partition {1} does not have a leader yet.", topic, p.PartitionId);
            }
            Logger.InfoFormat("RefreshMetadataInternal exit: {0} {1} {2} Topic:{3} ", versionId, clientId, correlationId, topic);
        }
        #endregion

        #region Offset
        /// <summary>
        ///  Get offset
        /// </summary>
        public void RefreshAndGetOffset(short versionId, string clientId, int correlationId, string topic, int partitionId, bool forceRefreshOffsetCache, out long earliestOffset, out long latestOffset)
        {
            earliestOffset = -1;
            latestOffset = -1;
            if (!forceRefreshOffsetCache && this.TopicOffsetEarliest.ContainsKey(topic) && this.TopicOffsetEarliest[topic].ContainsKey(partitionId))
            {
                earliestOffset = this.TopicOffsetEarliest[topic][partitionId];
            }
            if (!forceRefreshOffsetCache && this.TopicOffsetLatest.ContainsKey(topic) && this.TopicOffsetLatest[topic].ContainsKey(partitionId))
            {
                latestOffset = this.TopicOffsetLatest[topic][partitionId];
            }
            if (!forceRefreshOffsetCache && earliestOffset != -1 && latestOffset != -1)
                return;
            //Get
            using (Consumer consumer = this.GetConsumer(topic, partitionId))
            {
                Dictionary<string, List<PartitionOffsetRequestInfo>> offsetRequestInfoEarliest = new Dictionary<string, List<PartitionOffsetRequestInfo>>();
                List<PartitionOffsetRequestInfo> offsetRequestInfoForPartitionsEarliest = new List<PartitionOffsetRequestInfo>();
                offsetRequestInfoForPartitionsEarliest.Add(new PartitionOffsetRequestInfo(partitionId, OffsetRequest.EarliestTime, 1));
                offsetRequestInfoEarliest.Add(topic, offsetRequestInfoForPartitionsEarliest);
                OffsetRequest offsetRequestEarliest = new OffsetRequest(offsetRequestInfoEarliest);
                //Earliest
                OffsetResponse offsetResponseEarliest = consumer.GetOffsetsBefore(offsetRequestEarliest);
                List<PartitionOffsetsResponse> partitionOffsetEaliest = null;
                if (offsetResponseEarliest.ResponseMap.TryGetValue(topic, out partitionOffsetEaliest))
                {
                    foreach (var p in partitionOffsetEaliest)
                    {
                        if (p.Error == ErrorMapping.NoError && p.PartitionId == partitionId)
                        {
                            earliestOffset = p.Offsets[0];
                            //Cache                           
                            if (!this.TopicOffsetEarliest.ContainsKey(topic))
                                this.TopicOffsetEarliest.TryAdd(topic, new ConcurrentDictionary<int, long>());
                            this.TopicOffsetEarliest[topic][partitionId] = earliestOffset;
                        }
                    }
                }

                //Latest
                Dictionary<string, List<PartitionOffsetRequestInfo>> offsetRequestInfoLatest = new Dictionary<string, List<PartitionOffsetRequestInfo>>();
                List<PartitionOffsetRequestInfo> offsetRequestInfoForPartitionsLatest = new List<PartitionOffsetRequestInfo>();
                offsetRequestInfoForPartitionsLatest.Add(new PartitionOffsetRequestInfo(partitionId, OffsetRequest.LatestTime, 1));
                offsetRequestInfoLatest.Add(topic, offsetRequestInfoForPartitionsLatest);
                OffsetRequest offsetRequestLatest = new OffsetRequest(offsetRequestInfoLatest);

                OffsetResponse offsetResponseLatest = consumer.GetOffsetsBefore(offsetRequestLatest);
                List<PartitionOffsetsResponse> partitionOffsetLatest = null;
                if (offsetResponseLatest.ResponseMap.TryGetValue(topic, out partitionOffsetLatest))
                {
                    foreach (var p in partitionOffsetLatest)
                    {
                        if (p.Error == ErrorMapping.NoError && p.PartitionId == partitionId)
                        {
                            latestOffset = p.Offsets[0];
                            //Cache
                            if (!this.TopicOffsetLatest.ContainsKey(topic))
                                this.TopicOffsetLatest.TryAdd(topic, new ConcurrentDictionary<int, long>());
                            this.TopicOffsetLatest[topic][partitionId] = latestOffset;
                        }
                    }
                }
            }
        }
        public long RefreshAndGetOffsetByTimeStamp(short versionId, string clientId, int correlationId, string topic, int partitionId, DateTime timeStampInUTC)
        {
            //Get
            using (Consumer consumer = this.GetConsumer(topic, partitionId))
            {
                Dictionary<string, List<PartitionOffsetRequestInfo>> offsetRequestInfoEarliest = new Dictionary<string, List<PartitionOffsetRequestInfo>>();
                List<PartitionOffsetRequestInfo> offsetRequestInfoForPartitionsEarliest = new List<PartitionOffsetRequestInfo>();
                offsetRequestInfoForPartitionsEarliest.Add(new PartitionOffsetRequestInfo(partitionId, KafkaClientHelperUtils.ToUnixTimestampMillis(timeStampInUTC), 8));
                offsetRequestInfoEarliest.Add(topic, offsetRequestInfoForPartitionsEarliest);
                OffsetRequest offsetRequestEarliest = new OffsetRequest(offsetRequestInfoEarliest);
                //Earliest
                OffsetResponse offsetResponseEarliest = consumer.GetOffsetsBefore(offsetRequestEarliest);
                List<PartitionOffsetsResponse> partitionOffsetByTimeStamp = null;
                if (offsetResponseEarliest.ResponseMap.TryGetValue(topic, out partitionOffsetByTimeStamp))
                {
                    foreach (var p in partitionOffsetByTimeStamp)
                    {
                        if (p.PartitionId == partitionId)
                        {
                            return partitionOffsetByTimeStamp[0].Offsets[0];
                        }
                    }
                }
            }
            return -1;
        }

        #endregion


        #region Thread safe producer pool
        public int InitializeProducerPoolForTopic(short versionId, string clientId, int correlationId, string topic, bool forceRefreshMetadata, ProducerConfiguration producerConfigTemplate, bool forceRecreateEvenHostPortSame)
        {
            Logger.InfoFormat("InitializeProducerPoolForTopic ==  enter:  Topic:{0} forceRefreshMetadata:{1}  forceRecreateEvenHostPortSame:{2} ", topic, forceRefreshMetadata, forceRecreateEvenHostPortSame);
            TopicMetadata topicMetadata = null;
            if (forceRefreshMetadata)
            {
                topicMetadata = RefreshMetadata(versionId, clientId, correlationId, topic, forceRefreshMetadata);
            }

            Dictionary<int, Tuple<Broker, BrokerConfiguration>> partitionLeaders = this.TopicMetadataPartitionsLeaders[topic];

            //TODO: but some times the partition maybe has no leader 

            //For use partitioner calss.  Totally only create one producer. 
            if (string.IsNullOrEmpty(this.Config.PartitionerClass))
            {
                foreach (KeyValuePair<int, Tuple<Broker, BrokerConfiguration>> kv in partitionLeaders)
                {
                    CreateProducerOfOnePartition(topic, kv.Key, kv.Value.Item2, producerConfigTemplate, forceRecreateEvenHostPortSame);
                }
                Logger.InfoFormat("InitializeProducerPoolForTopic ==  exit:  Topic:{0} forceRefreshMetadata:{1}  forceRecreateEvenHostPortSame:{2} this.TopicPartitionsLeaderProducers[topic].Count:{3}", topic, forceRefreshMetadata, forceRecreateEvenHostPortSame, this.TopicPartitionsLeaderProducers[topic].Count);
                return this.TopicPartitionsLeaderProducers[topic].Count;
            }
            else
            {
                ProducerConfiguration producerConfig = new ProducerConfiguration(producerConfigTemplate);
                producerConfig.ZooKeeper = this.Config.ZookeeperConfig;
                producerConfig.PartitionerClass = this.Config.PartitionerClass;

                Producer<TKey, TData> oldProducer = null;
                if (TopicProducersWithPartitionerClass.TryGetValue(topic, out oldProducer))
                {
                    bool removeOldProducer = false;
                    if (oldProducer != null)
                    {
                        oldProducer.Dispose();
                        removeOldProducer = TopicProducersWithPartitionerClass.TryRemove(topic, out oldProducer);
                        Logger.InfoFormat("InitializeProducerPoolForTopic == Remove producer from TopicProducersWithPartitionerClass for  topic {0}  removeOldProducer:{1} ", topic, removeOldProducer);
                    }
                }

                Producer<TKey, TData> producer = new Producer<TKey, TData>(producerConfig);
                bool addNewProducer = TopicProducersWithPartitionerClass.TryAdd(topic, producer);
                Logger.InfoFormat("InitializeProducerPoolForTopic == Add producer  TopicProducersWithPartitionerClass for  topic {0}  SyncProducerOfOneBroker:{1} addNewProducer:{2}   END.", topic, producerConfig.SyncProducerOfOneBroker, addNewProducer);

                return addNewProducer ? 1 : 0;
            }
        }

        /// <summary>
        /// Get Producer once Config.PartitionerClass is one valid class full name.
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public Producer<TKey, TData> GetProducerWithPartionerClass(string topic)
        {
            if (string.IsNullOrEmpty(this.Config.PartitionerClass))
            {
                throw new ArgumentException("Please must specify KafkaSimpleManagerConfiguration.PartitionerClass before call this function.");
            }
            if (!this.TopicProducersWithPartitionerClass.ContainsKey(topic))
            {
                throw new KafkaException(string.Format("There is no  TopicProducersWithPartitionerClass producer  for topic {0}  , please make sure you have called CreateProducerForPartitions", topic));
            }

            return TopicProducersWithPartitionerClass[topic];
        }

        /// <summary>
        /// Get Producer once  Config.PartitionerClass is null or empty.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partitionID"></param>
        /// <param name="randomReturnIfProducerOfTargetPartionNotExists"></param>
        /// <returns></returns>
        public Producer<TKey, TData> GetProducerOfPartition(string topic, int partitionID, bool randomReturnIfProducerOfTargetPartionNotExists)
        {
            if (!string.IsNullOrEmpty(this.Config.PartitionerClass))
            {
                throw new ArgumentException(string.Format("Please call GetProducerWithPartionerClass to get producer since  KafkaSimpleManagerConfiguration.PartitionerClass is {0} is not null.", this.Config.PartitionerClass));
            }

            if (!this.TopicMetadatas.ContainsKey(topic))
            {
                throw new KafkaException(string.Format("There is no  metadata  for topic {0}.  Please call RefreshMetadata with force = true and try again. ", topic));
            }

            if (this.TopicMetadatas[topic].Error != ErrorMapping.NoError)
            {
                throw new KafkaException(string.Format("The metadata status for topic {0} is abnormal, detail: ", topic), this.TopicMetadatas[topic].Error);
            }

            if (!this.TopicPartitionsLeaderProducers.ContainsKey(topic))
            {
                throw new KafkaException(string.Format("There is no  producer  for topic {0} partition {1} , please make sure you have called CreateProducerForPartitions", topic, partitionID));
            }

            if (this.TopicPartitionsLeaderProducers[topic].ContainsKey(partitionID))
            {
                return this.TopicPartitionsLeaderProducers[topic][partitionID];
            }

            if (randomReturnIfProducerOfTargetPartionNotExists)
            {
                if (!this.TopicPartitionsLeaderProducers[topic].Any())
                {
                    return null;
                }

                int randomPartition = randomForGetCachedProducer.Next(this.TopicPartitionsLeaderProducers[topic].Count);
                return this.TopicPartitionsLeaderProducers[topic].Values.ElementAt(randomPartition);
            }
            else
                return null;
        }

        public Producer<TKey, TData> RefreshMetadataAndRecreateProducerOfOnePartition(short versionId, string clientId, int correlationId, string topic, int partitionId, bool forceRefreshMetadata, bool forceRecreateEvenHostPortSame, ProducerConfiguration producerConfigTemplate, bool randomReturnIfProducerOfTargetPartionNotExists)
        {
            Logger.InfoFormat("RefreshMetadataAndRecreateProducerWithPartition ==  enter:  Topic:{0}  partitionId:{1} forceRefreshMetadata:{2}  forceRecreateEvenHostPortSame:{3} randomReturnIfProducerOfTargetPartionNotExists:{4} ", topic, partitionId, forceRefreshMetadata, forceRecreateEvenHostPortSame, randomReturnIfProducerOfTargetPartionNotExists);

            TopicMetadata topicMetadata = null;
            if (forceRefreshMetadata)
            {
                topicMetadata = RefreshMetadata(versionId, clientId, correlationId, topic, forceRefreshMetadata);
            }

            if (!this.TopicMetadataPartitionsLeaders[topic].ContainsKey(partitionId))
                throw new NoLeaderForPartitionException(string.Format("No leader for topic {0} parition {1} ", topic, partitionId));

            Tuple<Broker, BrokerConfiguration> value = this.TopicMetadataPartitionsLeaders[topic][partitionId];

            CreateProducerOfOnePartition(topic, partitionId, value.Item2, producerConfigTemplate, forceRecreateEvenHostPortSame);

            return GetProducerOfPartition(topic, partitionId, randomReturnIfProducerOfTargetPartionNotExists);
        }

        private void CreateProducerOfOnePartition(string topic, int partitionId, BrokerConfiguration broker, ProducerConfiguration producerConfigTemplate, bool forceRecreateEvenHostPortSame)
        {
            Logger.InfoFormat("CreateProducer ==  enter:  Topic:{0} partitionId:{1} broker:{2}  forceRecreateEvenHostPortSame:{3} ", topic, partitionId, broker, forceRecreateEvenHostPortSame);
            //Explicitly set partitionID
            ProducerConfiguration producerConfig = new ProducerConfiguration(producerConfigTemplate, new List<BrokerConfiguration> { broker }, partitionId)
            {
                ForceToPartition = partitionId
            };

            if (!this.TopicPartitionsLeaderProducers.ContainsKey(topic))
            {
                this.TopicPartitionsLeaderProducers.TryAdd(topic, new ConcurrentDictionary<int, Producer<TKey, TData>>());
            }

            ConcurrentDictionary<int, Producer<TKey, TData>> dictPartitionLeaderProducersOfOneTopic = this.TopicPartitionsLeaderProducers[topic];
            bool needRecreate = true;
            Producer<TKey, TData> oldProducer = null;
            if (dictPartitionLeaderProducersOfOneTopic.TryGetValue(partitionId, out oldProducer))
            {
                if (oldProducer.Config.Brokers.Any()
                    && oldProducer.Config.Brokers[0].Equals(producerConfig.Brokers[0]))
                {
                    needRecreate = false;
                }
            }

            if (forceRecreateEvenHostPortSame)
            {
                needRecreate = true;
            }

            if (needRecreate)
            {
                lock (GetProduceLockOfTopic(topic, partitionId))
                {
                    if (dictPartitionLeaderProducersOfOneTopic.TryGetValue(partitionId, out oldProducer))
                    {
                        if (oldProducer.Config.Brokers.Any()
                            && oldProducer.Config.Brokers[0].Equals(producerConfig.Brokers[0]))
                        {
                            needRecreate = false;
                        }
                    }

                    if (forceRecreateEvenHostPortSame)
                        needRecreate = true;

                    if (!needRecreate)
                    {
                        Logger.InfoFormat("CreateProducer == Add producer SKIP  after got lock for  topic {0}  partition {1} since leader {2} not changed. Maybe created by other thread.  END.", topic, partitionId, producerConfig.Brokers[0]);
                        return;
                    }

                    bool removeOldProducer = false;
                    if (oldProducer != null)
                    {
                        oldProducer.Dispose();
                        removeOldProducer = dictPartitionLeaderProducersOfOneTopic.TryRemove(partitionId, out oldProducer);
                        Logger.InfoFormat("CreateProducer == Remove producer for  topic {0}  partition {1} leader {2} removeOldProducer:{3} ", topic, partitionId, oldProducer.Config.Brokers[0], removeOldProducer);
                    }

                    Producer<TKey, TData> producer = new Producer<TKey, TData>(producerConfig);
                    bool addNewProducer = dictPartitionLeaderProducersOfOneTopic.TryAdd(partitionId, producer);

                    Logger.InfoFormat("CreateProducer == Add producer for  topic {0}  partition {1} leader {2} SyncProducerOfOneBroker:{3} removeOldProducer:{4} addNewProducer:{5}   END.", topic, partitionId, broker, producerConfig.SyncProducerOfOneBroker, removeOldProducer, addNewProducer);
                }
            }
            else
                Logger.InfoFormat("CreateProducer == Add producer SKIP for  topic {0}  partition {1} since leader {2} not changed.   END.", topic, partitionId, producerConfig.Brokers[0]);
        }
        #endregion


        #region Get consumer object
        /// <summary>
        /// Get Consumer object from current cached metadata information without retry.
        /// So maybe got exception if the related metadata not exists.
        /// When create ConsumerConfiguration, will take BufferSize and FetchSize from KafkaSimpleManagerConfiguration
        /// Client side need handle exception and the metadata change 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partitionID"></param>
        /// <returns></returns>
        public Consumer GetConsumer(string topic, int partitionID)
        {
            return new Consumer(new ConsumerConfiguration()
            {
                Broker = GetLeaderBrokerOfPartition(topic, partitionID),
                BufferSize = this.Config.BufferSize,
                FetchSize = this.Config.FetchSize,
                Verbose = this.Config.Verbose,
            });
        }

        /// <summary>
        /// Get Consumer object from current cached metadata information without retry.
        /// So maybe got exception if the related metadata not exists.
        /// When create ConsumerConfiguration, will take values in cosumerConfigTemplate.
        /// Client side need handle exception and the metadata change 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partitionID"></param>
        /// <param name="cosumerConfigTemplate"></param>
        /// <returns></returns>
        public Consumer GetConsumer(string topic, int partitionID, ConsumerConfiguration cosumerConfigTemplate)
        {
            ConsumerConfiguration config = new ConsumerConfiguration(cosumerConfigTemplate, GetLeaderBrokerOfPartition(topic, partitionID));
            return new Consumer(config);
        }
        #endregion

        #region  Consumer pool
        /// <summary>
        /// MANIFOLD use .  get one consumer from the pool.
        /// </summary>
        public Consumer GetConsumerFromPool(short versionId, string clientId, int correlationId
                              , string topic, ConsumerConfiguration cosumerConfigTemplate, int partitionId)
        {
            if (!this.TopicPartitionsLeaderConsumers.ContainsKey(topic))
            {
                TopicMetadata topicMetadata = RefreshMetadata(versionId, clientId, correlationId, topic, false);
            }

            ConcurrentDictionary<int, Consumer> consumers = GetConsumerPoolForTopic(topic);
            if (!consumers.ContainsKey(partitionId))
            {
                lock (GetConsumeLockOfTopicPartition(topic, partitionId))
                {
                    if (!consumers.ContainsKey(partitionId))
                    {
                        ConsumerConfiguration config = new ConsumerConfiguration(cosumerConfigTemplate, GetLeaderBrokerOfPartition(topic, partitionId));
                        Consumer consumer = new Consumer(config);
                        if (consumers.TryAdd(partitionId, consumer))
                            Logger.InfoFormat("Create one consumer for client {0} topic {1} partitoin {2} addOneConsumer return value:{3} ", clientId, topic, partitionId, true);
                        else
                            Logger.WarnFormat("Create one consumer for client {0} topic {1} partitoin {2} addOneConsumer return value:{3} ", clientId, topic, partitionId, false);
                    }
                }
            }

            return consumers[partitionId];
        }

        /// <summary>
        /// MANIFOLD use .  Force recreate consumer for some partition and return it back.
        /// </summary>
        public Consumer GetConsumerFromPoolAfterRecreate(short versionId, string clientId, int correlationId
                              , string topic, ConsumerConfiguration cosumerConfigTemplate, int partitionId, int notRecreateTimeRangeInMs = -1)
        {
            TopicMetadata topicMetadata = RefreshMetadata(versionId, clientId, correlationId, topic, true);
            ConcurrentDictionary<int, Consumer> consumers = GetConsumerPoolForTopic(topic);
            lock (GetConsumeLockOfTopicPartition(topic, partitionId))
            {
                ConsumerConfiguration config = new ConsumerConfiguration(cosumerConfigTemplate, GetLeaderBrokerOfPartition(topic, partitionId));
                Consumer oldConsumer = null;
                if (consumers.TryGetValue(partitionId, out oldConsumer))
                {
                    if ((DateTime.UtcNow.Ticks - oldConsumer.CreatedTimeInUTC) / 10000.0 < notRecreateTimeRangeInMs)
                    {
                        Logger.WarnFormat("Do NOT recreate consumer for client {0} topic {1} partitoin {2} since it only created {3} ms. less than {4} ", clientId, topic, partitionId
                            , (DateTime.UtcNow.Ticks - oldConsumer.CreatedTimeInUTC) / 10000.0, notRecreateTimeRangeInMs);
                    }
                    else
                    {
                        Logger.InfoFormat("Destroy one old consumer for client {0} topic {1} partitoin {2} ", clientId, topic, partitionId);
                        if (oldConsumer != null)
                        {
                            oldConsumer.Dispose();
                        }

                        consumers[partitionId] = new Consumer(config);
                        Logger.InfoFormat("Create one consumer for client {0} topic {1} partitoin {2} ", clientId, topic, partitionId);
                    }
                }
                else
                {
                    consumers[partitionId] = new Consumer(config);
                    Logger.InfoFormat("Newly Create one consumer for client {0} topic {1} partitoin {2} ", clientId, topic, partitionId);
                }
            }

            return consumers[partitionId];
        }
        private ConcurrentDictionary<int, Consumer> GetConsumerPoolForTopic(string topic)
        {
            if (this.TopicPartitionsLeaderConsumers.ContainsKey(topic))
                return this.TopicPartitionsLeaderConsumers[topic];

            this.TopicPartitionsLeaderConsumers.TryAdd(topic, new ConcurrentDictionary<int, Consumer>());

            return this.TopicPartitionsLeaderConsumers[topic];
        }
        #endregion

        #region Dispose
        /// <summary>
        /// Implement IDisposable
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            // This object will be cleaned up by the Dispose method.
            // Therefore, you should call GC.SupressFinalize to
            // take this object off the finalization queue
            // and prevent finalization code for this object
            // from executing a second time.
            GC.SuppressFinalize(this);
        }

        // Dispose(bool disposing) executes in two distinct scen/arios.
        // If disposing equals true, the method has been called directly
        // or indirectly by a user's code. Managed and unmanaged resources
        // can be disposed.
        // If disposing equals false, the method has been called by the
        // runtime from inside the finalizer and you should not reference
        // other objects. Only unmanaged resources can be disposed.
        protected virtual void Dispose(bool disposing)
        {
            // Check to see if Dispose has already been called.
            if (!this.disposed)
            {
                // If disposing equals true, dispose all managed
                // and unmanaged resources.
                if (disposing)
                {
                    // Dispose managed resources.
                    this.ClearSyncProducerPoolForMetadata();
                }

                // Call the appropriate methods to clean up
                // unmanaged resources here.
                // If disposing is false,
                // only the following code is executed.
                // Note disposing has been done.
                disposed = true;
            }
        }
        #endregion

        #region lock by topic
        private object GetProduceLockOfTopic(string topic, int partitionId)
        {
            object lockOfTopicPartition = null;
            string key = topic + "_" + partitionId.ToString();
            while (!TopicLockProduce.TryGetValue(key, out lockOfTopicPartition))
            {
                lockOfTopicPartition = new object();
                if (TopicLockProduce.TryAdd(key, lockOfTopicPartition))
                    break;
            }
            return lockOfTopicPartition;
        }
        private object GetConsumeLockOfTopicPartition(string topic, int partitionId)
        {
            object lockOfTopicPartition = null;
            string key = topic + "_" + partitionId.ToString();

            while (!TopicPartitionLockConsume.TryGetValue(key, out lockOfTopicPartition))
            {
                lockOfTopicPartition = new object();
                if (TopicPartitionLockConsume.TryAdd(key, lockOfTopicPartition))
                    break;
            }
            return lockOfTopicPartition;
        }
        #endregion
    }
}
