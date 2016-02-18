namespace Kafka.Client.Helper
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Threading;

    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;

    /// <summary>
    /// Temporaly add back for VERITAS
    /// This class should be obsolete.
    /// Please use KafkaSimpleManger and KafkaSimpleManagerConfiguration    
    /// Manage Producer and Consumer configuration for one topic.
    /// Need use using while use this class.
    /// </summary>
    [CLSCompliant(false)]
    public class KafkaClientHelperWrapper : IDisposable
    {
        public static log4net.ILog Logger= log4net.LogManager.GetLogger(typeof(KafkaClientHelperWrapper));

        public IEnumerable<PartitionMetadata> PartitionsMetadata { get; private set; }

        public int PartitionCount
        {
            get
            {
                return this.PartitionsMetadata == null ? 0 : Enumerable.Count<PartitionMetadata>(this.PartitionsMetadata);
            }
        }

        public KafkaClientHelperConfiguration HelperConfiguration { get; private set; }

        public IDictionary<int, BrokerConfiguration> Leaders { get { return this.leaders; } }

        // the pool of syncProducer for TopicMetaData requests, which retrieve PartitionMetaData, including leaders and ISRs.
        private SyncProducerPool syncProducerPool = null;
        private List<ISyncProducer> syncProducerList = null;
        private int producerIndex = -1;
        private DateTime lastTimeBrokerListUpdated = DateTime.Now.AddYears(-1000);

        private string topic = string.Empty;
        private TopicMetadataRequest topicMetaRequest = null;

        // partitionIndex as the key; track leader for each partition
        private Dictionary<int, BrokerConfiguration> leaders = new Dictionary<int, BrokerConfiguration>();
        private DateTime lastTimeLeaderFound = DateTime.Now.AddYears(-1000);

        // brokerId as the key; producer configuration and producer for each broker
        private Dictionary<int, ProducerConfiguration> producerConfs = new Dictionary<int, ProducerConfiguration>();
        private Dictionary<int, Producer<int, Message>> producers = new Dictionary<int, Producer<int, Kafka.Client.Messages.Message>>();

        private Random random = new Random();

        // Track whether Dispose has been called.
        private bool disposed = false;

        /// <summary>
        /// KafkaClientHelperWrapper produces or consumes messages to/from a particular partition of a topic
        /// </summary>
        /// <param name="topic">target topic</param>
        /// <param name="helperConfiguration"></param>
        public KafkaClientHelperWrapper(string topic, KafkaClientHelperConfiguration helperConfiguration)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new System.ArgumentNullException("topicParam");
            }

            if (helperConfiguration == null)
            {
                throw new System.ArgumentNullException("helperConfiguration");
            }

            if (helperConfiguration.LeaderConfig == null && string.IsNullOrEmpty(helperConfiguration.KafkaBrokerList) && helperConfiguration.ZookeeperConfig == null)
            {
                throw new System.ArgumentException("Leader and KafkaBrokerList and Zookeepr connection string are missing");
            }

            Logger.DebugFormat("KafkaClientHelperWrapper constructor start,topicParam={0},kafkaBrokerList={1},zookeeper={2}",
                topic, helperConfiguration.KafkaBrokerList, helperConfiguration.ZookeeperConfig);

            ServicePointManager.DefaultConnectionLimit = 5000;
            ServicePointManager.UseNagleAlgorithm = false;
            this.topic = topic;
            this.HelperConfiguration = helperConfiguration;

            if (this.HelperConfiguration.LeaderConfig != null)
            {
                this.leaders.Add(this.HelperConfiguration.LeaderPartition, this.HelperConfiguration.LeaderConfig);
                this.lastTimeLeaderFound = DateTime.Now;
            }

            this.topicMetaRequest = TopicMetadataRequest.Create(
                new string[] { this.topic },                                    // topic
                0,                                                              // API version id
                this.random.Next(int.MinValue, int.MaxValue),                   // correlation id 
                Assembly.GetExecutingAssembly().ManifestModule.ToString());     // client id
        }

        /// <summary>
        /// Return ConsumerConfiguration for the targetPartition
        /// </summary>
        public ConsumerConfiguration GetConsumerConf(int partitionIndex)
        {
            this.ValidateConsumerLeaderInitialization(partitionIndex);

            // BufferSize default=65536; FetchSize default=1048576; MaxFetchsize=FetchSize * 10; Timeout default=-1; SocketTimeout=30 * 1000
            // Debug value: SocketTimeout = 3600 * 1000, Timeout = 3600 * 1000
            return new ConsumerConfiguration()
            {
                Timeout = this.HelperConfiguration.Timeout + 10,
                ReceiveTimeout = this.HelperConfiguration.Timeout + 20,
                Broker = this.leaders[partitionIndex],
                BufferSize = this.HelperConfiguration.MaxMessageSize,
                FetchSize = this.HelperConfiguration.MaxMessageSize,
                AutoCommit = false
            };
        }

        /// <summary>
        /// Return Consumer for the targetPartition
        /// </summary>
        /// <returns></returns>
        public Consumer GetKafkaConsumer(int partitionIndex)
        {
            this.ValidateConsumerLeaderInitialization(partitionIndex);

            return new Consumer(this.GetConsumerConf(partitionIndex), this.leaders[partitionIndex].Host, this.leaders[partitionIndex].Port);
        }

        /// <summary>
        /// Return ProducerConfiguration for the targetPartition
        /// </summary>
        /// <param name="partitionParam"></param>
        /// <returns></returns>
        public ProducerConfiguration GetProducerConf(int partitionParam)
        {
            int targetPartition = this.ValidateProducerLeaderTargetPartitionInialization(partitionParam);

            if (this.producerConfs.Count == 0 || !this.producerConfs.ContainsKey(this.leaders[targetPartition].BrokerId))
            {


            }

            return this.producerConfs[this.leaders[targetPartition].BrokerId];
        }

        /// <summary>
        /// Return Producer for the targetPartition
        /// </summary>
        /// <param name="partitionParam"></param>
        /// <returns></returns>
        public Producer<int, Message> GetKafkaProducer(int partitionParam,
            CompressionCodecs comparessionCodec,
            out ProducerConfiguration producerConfiguration)
        {
            int targetPartition = this.ValidateProducerLeaderTargetPartitionInialization(partitionParam);

            if (this.producers.Count == 0 || !this.producers.ContainsKey(this.leaders[targetPartition].BrokerId))
            {

                Logger.DebugFormat("GetProducerConf creatNew for leaderBrokerId={0},leaders.Count={1},targetPartition={2},leaderBrokerDetails={3}",
                   this.leaders[targetPartition].BrokerId, this.leaders.Count(), targetPartition, this.leaders[targetPartition]);

                // AckTimeout deafult=1, MaxMessageSize default = 1000000; ConnectionTimout default=5000; BufferSize default=102400, DefaultSocketTimeout=30 * 1000
                ProducerConfiguration producerConf = new ProducerConfiguration(new List<BrokerConfiguration>() { this.leaders[targetPartition] })
                {
                    // PartitionerClass = "Kafka.Client.Producers.Partitioning.ModPartitioner",
                    PartitionerClass = ProducerConfiguration.DefaultPartitioner,
                    //
                    // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceRequest
                    // RequiredAcks: This field indicates how many acknowledgements the servers should receive before responding to the request.
                    //     If it is 0 the server will not send any response (this is the only case where the server will not reply to a request). 
                    //     If it is 1, the server will wait the data is written to the local log before sending a response. 
                    //     If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. 
                    //     For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for 
                    //         more acknowledgements than there are in-sync replicas). 
                    // Timeout: This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks. 
                    //      The timeout is not an exact limit on the request time for a few reasons:
                    //    (1) it does not include network latency, 
                    //    (2) the timer begins at the beginning of the processing of this request so if many requests are queued due to server overload that wait time will not be included, 
                    //    (3) we will not terminate a local write so if the local write time exceeds this timeout it will not be respected. To get a hard timeout of this type the client 
                    //         should use the socket timeout. 
                    RequiredAcks = -1,
                    CompressionCodec = comparessionCodec,
                    SendTimeout = this.HelperConfiguration.Timeout,
                    ReceiveTimeout = this.HelperConfiguration.Timeout,
                    AckTimeout = this.HelperConfiguration.AckTimeout,
                    MaxMessageSize = this.HelperConfiguration.MaxMessageSize,
                    BufferSize = this.HelperConfiguration.MaxMessageSize,
                    TotalNumPartitions = this.PartitionsMetadata != null ? this.PartitionsMetadata.Count() : this.HelperConfiguration.TotalNumPartitions
                };

                this.producerConfs.Add(this.leaders[targetPartition].BrokerId, producerConf);

                this.producers.Add(this.leaders[targetPartition].BrokerId
                    , new Producer<int, Kafka.Client.Messages.Message>(producerConf));
            }

            producerConfiguration = this.producerConfs[this.leaders[targetPartition].BrokerId];
            return this.producers[this.leaders[targetPartition].BrokerId];
        }

        /// <summary>
        /// Find a leader broker by issuing a topicMetaReqeust
        /// </summary>
        /// <returns>number of leaders
        public int CallKafkaAndFindLeader()
        {
            string s = string.Empty;

            // retry at least once
            int maxRetry = 1;
            int retryCount = 0;
            while (this.leaders.Count == 0 && retryCount < maxRetry)
            {
                SyncProducerConfiguration producerConfig = null;
                try
                {
                    Console.WriteLine("in try catch block");
                    if (this.syncProducerList == null)
                    {
                        // initialize SyncProducerPool 
                        this.InitSyncProducerPool();
                    }

                    // retry each broker two times. Observed non-leader broker responds with error message, and some leader knows only a small set of partitions 
                    maxRetry = this.syncProducerList == null ? maxRetry : this.syncProducerList.Count * 2;
                    if (retryCount > 0)
                    {
                        // not the first time, rotate the producer, wait a few seconds, before retry
                        this.RoundRobinSyncProducer(this.syncProducerList.Count);
                        Thread.Sleep(this.HelperConfiguration.MaxRetryWaitTime);
                    }

                    producerConfig = this.syncProducerList[this.producerIndex].Config;
                    Console.WriteLine("got producer config");
                    IEnumerable<TopicMetadata> topicMetaData = this.syncProducerList[this.producerIndex].Send(this.topicMetaRequest);
                    Console.WriteLine("got topic meta data");
                    if (topicMetaData.Count() == 0)
                    {
                        s = "FindLeader found ZERO count topicMetaData,topic={0},producerIndex[{1}]=[{2}]";
                        Logger.ErrorFormat(s, this.topic, this.producerIndex, KafkaClientHelperUtils.SyncProducerConfigToString(producerConfig));
                        continue;
                    }
                    else if (topicMetaData.First().PartitionsMetadata.Count() == 0)
                    {
                        s = "FindLeader found ZERO count partitionMetaData,topic={0},producerIndex[{1}]=[{2}]";
                        Logger.ErrorFormat(s, this.topic, this.producerIndex, KafkaClientHelperUtils.SyncProducerConfigToString(producerConfig));
                        continue;
                    }
                    else if (topicMetaData.Count() > 1)
                    {
                        s = "FindLeader found more than one topicData,topicMetaData.Count={0},topic={1},producerIndex[{2}]=[{3}],will use first one - expect only one";
                        Logger.ErrorFormat(s, topicMetaData.Count(), this.topic, this.producerIndex, KafkaClientHelperUtils.SyncProducerConfigToString(producerConfig));
                    }

                    this.PartitionsMetadata = topicMetaData.First().PartitionsMetadata.OrderBy(r => r.PartitionId);
                    this.leaders.Clear();
                    foreach (var m in this.PartitionsMetadata)
                    {
                        this.leaders.Add(m.PartitionId, new BrokerConfiguration()
                        {
                            BrokerId = m.Leader.Id,
                            Host = m.Leader.Host,
                            Port = m.Leader.Port
                        });
                    }

                    if (this.leaders.Count == 0)
                    {
                        s = "no leader found,topic={0},producerIndex[{1}]=[{2}],retryCount={3}";
                        Logger.ErrorFormat(s, this.topic, this.producerIndex, KafkaClientHelperUtils.SyncProducerConfigToString(producerConfig), retryCount);
                        continue;
                    }
                    else
                    {
                        s = "KafkaClientHelperWrapper[{0}] leader found,leaders.Count={1},topic={2},producerIndex[{3}]=[{4}],retryCount={5}";
                        Logger.DebugFormat(s, this.GetHashCode().ToString("X"), this.leaders.Count, this.topic, this.producerIndex,
                            KafkaClientHelperUtils.SyncProducerConfigToString(producerConfig), retryCount);
                        this.lastTimeLeaderFound = DateTime.Now;
                    }
                }
                catch (Exception e)
                {
                    s = "FindLeader hit exception,topic={0}, producerIndex[{1}]=[{2}],retryCount={3},maxRetry={4}";
                    Logger.Error(string.Format(s, this.topic, this.producerIndex, KafkaClientHelperUtils.SyncProducerConfigToString(producerConfig), retryCount, maxRetry), e);

                    if (retryCount >= maxRetry)
                    {
                        // reach maximum retry, bail
                        throw;
                    }
                }
                finally
                {
                    ++retryCount;
                }
            } // end of while loop

            return this.leaders.Count;
        }

        /// <summary>
        /// Implement IDisposable
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
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
                    this.ClearSyncProducerPool();
                    this.ClearLeader();
                    this.topic = string.Empty;
                    this.topicMetaRequest = null;
                    this.random = null;
                }

                // Call the appropriate methods to clean up
                // unmanaged resources here.
                // If disposing is false,
                // only the following code is executed.
                // Note disposing has been done.
                this.disposed = true;
            }
        }

        /// <summary>
        /// clear the leader and configurations dependent on the leader
        /// </summary>
        private void ClearLeader()
        {
            this.producerConfs.Clear();
            if (this.producers.Count > 0)
            {
                foreach (var v in this.producers)
                {
                    v.Value.Dispose();
                }
                this.producers.Clear();
            }

            this.leaders.Clear();
            this.PartitionsMetadata = null;
            Logger.DebugFormat("KafkaClientHelperWrapper[{0}] Leader cleared.", this.GetHashCode().ToString("X"));
        }

        /// <summary>
        /// Initialize SyncProducerPool with either the list of brokers provided by the caller or query Zookeeper
        /// </summary>
        private void InitSyncProducerPool()
        {
            if (this.syncProducerList != null)
            {
                // already initialized
                return;
            }

            if (!string.IsNullOrEmpty(this.HelperConfiguration.KafkaBrokerList))
            {
                // Honor KafkaBrokerList first
                this.syncProducerList = new List<ISyncProducer>();
                string[] brokers = this.HelperConfiguration.KafkaBrokerList.Split(new char[] { ',' });

                int i = 0;
                foreach (string v in brokers)
                {
                    i++;
                    string[] brokerParams = v.Split(new char[] { ':' });
                    int port = 0;
                    if (brokerParams.Count() == 2 && !string.IsNullOrEmpty(brokerParams[0]) && int.TryParse(brokerParams[1], out port))
                    {
                        var syncProducer = KafkaClientHelperUtils.TryCreateSyncProducer(i, brokerParams[0], port);
                        if (syncProducer != null)
                        {
                            this.syncProducerList.Add(syncProducer);
                        }
                    }
                }
            }
            else if (this.HelperConfiguration.ZookeeperConfig != null)
            {
                // Honor Zookeeper connection string only when KafkaBroker list not provided
                var producerConfig = new ProducerConfiguration(new List<BrokerConfiguration>());
                producerConfig.ZooKeeper = this.HelperConfiguration.ZookeeperConfig;
                //Backward compatible, so set to 1.
                producerConfig.SyncProducerOfOneBroker = 1;
                this.syncProducerPool = new SyncProducerPool(producerConfig);
                this.syncProducerList = this.syncProducerPool.GetShuffledProducers();
            }
            else if (this.HelperConfiguration.LeaderConfig != null)
            {
                // if only leader is provided, the leader to be added to syncProducerList 
                var leader = this.HelperConfiguration.LeaderConfig;
                this.syncProducerList = new List<ISyncProducer>() { new SyncProducer(new SyncProducerConfiguration() { BrokerId = leader.BrokerId, Host = leader.Host, Port = leader.Port }) };
            }

            if (this.syncProducerList == null || this.syncProducerList.Count == 0)
            {
                string s = string.Format("KafkaClientHelperWrapper[{0}] SyncProducerPool Initialization produced empty syncProducer list,Kafka={1},Zookeeper={2}",
                    this.GetHashCode().ToString("X"), this.HelperConfiguration.KafkaBrokerList, this.HelperConfiguration.ZookeeperConfig);

                Logger.Debug(s);
                this.syncProducerList = null;
                throw new ArgumentException(s);
            }

            this.RoundRobinSyncProducer(this.syncProducerList.Count);
            this.lastTimeBrokerListUpdated = DateTime.Now;
            Logger.DebugFormat("KafkaClientHelperWrapper[{0}] SyncProducerPool initialized", this.GetHashCode().ToString("X"));
        }

        /// <summary>
        /// Reset syncProducer pool
        /// </summary>
        private void ClearSyncProducerPool()
        {
            if (this.syncProducerList != null)
            {
                this.syncProducerList.Clear();
                this.syncProducerList = null;
            }

            if (this.syncProducerPool != null)
            {
                this.syncProducerPool.Dispose();
                this.syncProducerPool = null;
            }

            this.producerIndex = 0;
            Logger.DebugFormat("KafkaClientHelperWrapper[{0}] SyncProducerPool cleared", this.GetHashCode().ToString("X"));
        }

        public int RoundRobinSyncProducer()
        {
            return this.RoundRobinSyncProducer(this.syncProducerList.Count);
        }

        private int RoundRobinSyncProducer(int count)
        {
            if (this.producerIndex == -1)
            {
                this.producerIndex = this.random.Next(0, count);
            }
            else
            {
                this.producerIndex = (this.producerIndex + 1) % count;
            }
            return this.producerIndex;
        }

        /// <summary>
        /// Ensure the leader for the producer of the target partition is initialized /
        /// </summary>
        /// <param name="partitionIndex"></param>
        /// <returns>targetPartition</returns>
        private int ValidateProducerLeaderTargetPartitionInialization(int partitionIndex)
        {
            if (this.leaders == null || this.leaders.Count == 0 || !this.leaders.ContainsKey(partitionIndex))
            {
                throw new IndexOutOfRangeException(string.Format("leaders not initialized,leadersCount={0},targetPartition={1}", this.leaders.Count, partitionIndex));
            }

            return partitionIndex;
        }

        /// <summary>
        /// Ensure the leader for the consumer is initialized
        /// </summary>
        private void ValidateConsumerLeaderInitialization(int partitionIndex)
        {
            if (partitionIndex < 0)
            {
                throw new IndexOutOfRangeException("For a consumer, partitionIndex must not be negative");
            }

            if (this.leaders == null || this.leaders.Count == 0 || !this.leaders.ContainsKey(partitionIndex))
            {
                throw new IndexOutOfRangeException("leaders not initialized");
            }
        }
    }
}
