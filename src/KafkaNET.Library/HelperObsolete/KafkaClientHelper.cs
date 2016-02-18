namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;


    /// <summary>
    /// This class should be obsolete.
    /// Please use KafkaSimpleManger and KafkaSimpleManagerConfiguration    
    /// </summary>
    public class KafkaClientHelper : IDisposable
    {
        public static log4net.ILog Logger= log4net.LogManager.GetLogger(typeof(KafkaClientHelper)); 

        private List<BrokerConfiguration> brokerConfList = null;
        private SyncProducer[] syncProducerPool = null;
        private int producerIndex = -1;

        private string topic = string.Empty;
        private TopicMetadataRequest topicMetaRequest = null;
        private int partitionIndex = -1;
        private IList<BrokerConfiguration> leaders = new List<BrokerConfiguration>();
        private ProducerConfiguration producerConf = null;
        private Producer<int, Kafka.Client.Messages.Message> producer = null;
        private ConsumerConfiguration consumerConf = null;
        private Consumer consumer = null;
        private Random random = new Random();
        // Track whether Dispose has been called.
        private bool disposed = false;
        private KafkaClientHelperConfiguration helperConfiguration;

        public KafkaClientHelper(string topicParam, int partitionIndex, string kafkaBrokerList, KafkaClientHelperConfiguration helperConfiguration)
            :this(topicParam,partitionIndex,helperConfiguration)
        {
            if (!string.IsNullOrEmpty(kafkaBrokerList)
                && !string.IsNullOrEmpty(helperConfiguration.KafkaBrokerList)
                && kafkaBrokerList.ToUpperInvariant() != helperConfiguration.KafkaBrokerList.ToUpperInvariant())
            {
                throw new ArgumentException(string.Format("KafkaBrokerList doesn't match, please double check. {0} != {1}", kafkaBrokerList, helperConfiguration.KafkaBrokerList));
            }
        }
        public KafkaClientHelper(string topicParam, int partitionIndex, KafkaClientHelperConfiguration helperConfiguration)
        {
            ServicePointManager.DefaultConnectionLimit = 5000;
            ServicePointManager.UseNagleAlgorithm = false;

            this.partitionIndex = partitionIndex;
            this.helperConfiguration = helperConfiguration;
            this.MaxMessageSize = helperConfiguration.MaxMessageSize;
            this.MaxMessagePerSet = helperConfiguration.MaxMessagePerSet;
            this.ConsumerMaxWait = helperConfiguration.MaxWaitTime;
            this.Offset = helperConfiguration.Offset;
            this.OffsetType = helperConfiguration.OffsetType;

           // Logger.Debug(string.Format("KafkaClientHelper constructor start,topicParam={0},partitionIndex={1},kafkaBrokerList={2}", topicParam, partitionIndex, kafkaBrokerList));

            if (string.IsNullOrEmpty(this.helperConfiguration.KafkaBrokerList))
            {
                throw new System.ArgumentNullException("kafkaBrokerList");
            }

            if (string.IsNullOrEmpty(topicParam))
            {
                throw new System.ArgumentNullException("topicParam");
            }

            this.topic = topicParam;

            // assemble brokerConfig list
            this.brokerConfList = new List<BrokerConfiguration>(5);

            string[] brokers = this.helperConfiguration.KafkaBrokerList.Split(new char[] { ',' });

            int i = 1;
            foreach (string v in brokers)
            {
                string[] brokerParams = v.Split(new char[] { ':' });
                int port = 0;
                if (brokerParams.Count() == 2
                    && !string.IsNullOrEmpty(brokerParams[0])
                    && int.TryParse(brokerParams[1], out port))
                {
                    this.brokerConfList.Add(new BrokerConfiguration() { BrokerId = i, Host = brokerParams[0], Port = port });
                }

                i++;
            }

            // prepare SyncProducer list
            i = 0;
            syncProducerPool = new SyncProducer[this.brokerConfList.Count];
            this.RoundRobinSyncProducer();

            topicMetaRequest = TopicMetadataRequest.Create(
                new string[] { this.topic },                                    // topic
                0,                                                              // API version id
                this.random.Next(int.MinValue, int.MaxValue),                   // correlation id 
                Assembly.GetExecutingAssembly().ManifestModule.ToString());     // client id
        }

        public int ConsumerMaxWait { get; set; }

        public int MaxMessageSize { get; set; }

        public int MaxMessagePerSet { get; set; }

        public long Offset { get; set; }

        public KafkaOffsetType OffsetType { get; set; }

        public IEnumerable<PartitionMetadata> PartitionsMetadata { get; private set; }

        /// <summary>
        /// Ensure consumerConf is set
        /// </summary>
        [CLSCompliantAttribute(false)]
        public ConsumerConfiguration ConsumerConf
        {
            get
            {
                if (this.consumerConf == null)
                {
                    if (this.leaders.Count == 0)
                    {
                        throw new IndexOutOfRangeException("leaders not initialized");
                    }

                    // BufferSize default=65536; FetchSize default=1048576; MaxFetchsize=FetchSize * 10; Timeout default=-1; SocketTimeout=30 * 1000
                    // Debug value: SocketTimeout = 3600 * 1000, Timeout = 3600 * 1000
                    consumerConf = new ConsumerConfiguration()
                    {
                        Timeout = this.ConsumerMaxWait + 10,
                        // SocketTimeout = this.ConsumerMaxWait + 20,
                        ReceiveTimeout = this.ConsumerMaxWait + 20,
                        Broker = this.leaders[0],
                        BufferSize = this.MaxMessageSize + helperConfiguration.BufferExtra,
                        FetchSize = this.MaxMessageSize + helperConfiguration.BufferExtra,
                        AutoCommit = false
                    };
                }
                return this.consumerConf;
            }

        }

        [CLSCompliantAttribute(false)]
        public Consumer KafkaConsumer
        {
            get
            {
                if (this.consumer == null)
                {
                    if (this.leaders.Count == 0)
                    {
                        throw new IndexOutOfRangeException("leaders not initialized");
                    }

                    this.consumer = new Consumer(this.ConsumerConf, leaders[0].Host, leaders[0].Port);
                }

                return this.consumer;
            }
        }

        [CLSCompliantAttribute(false)]
        public ProducerConfiguration ProducerConf
        {
            get
            {
                if (this.producerConf == null)
                {
                    if (this.leaders.Count == 0)
                    {
                        throw new IndexOutOfRangeException("leaders not initialized");
                    }

                    // AckTimeout deafult=1, MaxMessageSize default = 1000000; ConnectionTimout default=5000; BufferSize default=102400, DefaultSocketTimeout=30 * 1000
                    this.producerConf = new ProducerConfiguration(leaders)
                    {
                        // PartitionerClass = "Kafka.Client.Producers.Partitioning.ModPartitioner",
                        PartitionerClass = ProducerConfiguration.DefaultPartitioner,
                        RequiredAcks = 1,
                        CompressionCodec = CompressionCodecs.NoCompressionCodec,
                        // SocketTimeout = this.ConsumerMaxWait + 20,
                        SendTimeout = this.ConsumerMaxWait + 20,
                        AckTimeout = this.ConsumerMaxWait,
                        ConnectTimeout = this.ConsumerMaxWait + 10,
                        MaxMessageSize = this.MaxMessageSize + helperConfiguration.BufferExtra,
                        BufferSize = this.MaxMessageSize + helperConfiguration.BufferExtra
                    };
                }

                return this.producerConf;
            }
        }

        [CLSCompliantAttribute(false)]
        public Producer<int, Kafka.Client.Messages.Message> KafkaProducer
        {
            get
            {
                if (this.producer == null)
                {
                    this.producer = new Producer<int, Kafka.Client.Messages.Message>(this.ProducerConf);
                }
                return this.producer;
            }
        }

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
                    if (syncProducerPool != null)
                    {
                        foreach (var v in syncProducerPool)
                        {
                            if (v != null)
                            {
                                v.Dispose();
                            }
                        }
                        syncProducerPool = null;
                    }

                    if (this.brokerConfList != null)
                    {
                        this.brokerConfList.Clear();
                        this.brokerConfList = null;
                    }

                    this.topic = string.Empty;
                    topicMetaRequest = null;
                    random = null;

                    this.ClearLeader();
                }

                // Call the appropriate methods to clean up
                // unmanaged resources here.
                // If disposing is false,
                // only the following code is executed.

                // Note disposing has been done.
                disposed = true;
            }
        }

        /// <summary>
        /// clear the leader and configurations dependent on the leader
        /// </summary>
        public void ClearLeader()
        {
            if (this.producer != null)
            {
                this.producer.Dispose();
                this.producer = null;
            }

            if (this.consumer != null)
            {
                this.consumer.Dispose();
                this.consumer = null;
            }

            this.producerConf = null;
            this.consumerConf = null;
            this.leaders.Clear();

            if (this.PartitionsMetadata != null)
            {
                this.PartitionsMetadata = null;
            }
        }

        /// <summary>
        /// Find a leader broker by issuing a topicMetaReqeust
        /// </summary>
        /// <returns>number of leaders
        public int FindLeader()
        {
            // retry each broker two times
            int maxRetry = this.brokerConfList.Count * 2;
            int retryCount = 0;
            // IList<BrokerConfiguration> leaders = new List<BrokerConfiguration>();
            HashSet<int> leaderBrokerIds = new HashSet<int>();
            string s = string.Empty;

            while (this.leaders.Count == 0)
            {
                if (retryCount > 0)
                {
                    // not the first time, rotate the producer, wait a few seconds, before retry
                    this.RoundRobinSyncProducer();
                    Thread.Sleep(helperConfiguration.MaxRetryWaitTime);
                }

                try
                {
                    if (this.syncProducerPool[this.producerIndex] == null)
                    {
                        var v = this.brokerConfList[this.producerIndex];

                        // RequiredAcks default=0
                        // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI 
                        // 0: the server will not send any response (this is the only case where the server will not reply to a request). 
                        // 1: the server will wait the data is written to the local log before sending a response. 
                        // -1: the server will block until the message is committed by all in sync replicas before sending a response. 
                        // For any number > 1 the server will block waiting for this number of acknowledgements to 
                        // occur (but the server will never wait for more acknowledgements than there are in-sync replicas). 
                        // Note: AckTimeout deafult=1, MaxMessageSize default = 1000000; ConnectionTimout default=5000; BufferSize default=102400, DefaultSocketTimeout=30 * 1000
                        this.syncProducerPool[this.producerIndex] = new SyncProducer(
                            new SyncProducerConfiguration()
                            {
                                BrokerId = v.BrokerId,
                                Host = v.Host,
                                Port = v.Port,
                                RequiredAcks = 1,
                                // SocketTimeout = this.ConsumerMaxWait + 20,
                                SendTimeout = this.ConsumerMaxWait + 20,
                                AckTimeout = this.ConsumerMaxWait,
                                ConnectTimeout = this.ConsumerMaxWait + 10,
                                MaxMessageSize = this.MaxMessageSize,
                                BufferSize = this.MaxMessageSize
                            });
                    }

                    var topicMetaData = syncProducerPool[this.producerIndex].Send(topicMetaRequest);

                    if (topicMetaData.Count() == 0)
                    {
                        s = string.Format("FindLeader found topicMetaData.Count=0,topic={0},producerIndex[{1}]={2}", this.topic, this.producerIndex, this.brokerConfList[this.producerIndex].ToString());
                        Logger.Error(s);
                        continue;
                    }
                    else if (topicMetaData.First().PartitionsMetadata.Count() == 0)
                    {
                        s = string.Format("FindLeader found partitionMetaData.Count=0,topic={0},producerIndex[{1}]={2}", this.topic, this.producerIndex, this.brokerConfList[this.producerIndex].ToString());
                        Logger.Error(s);
                        continue;
                    }
                    else if (topicMetaData.Count() > 1)
                    {
                        s = "FindLeader found more than one topicData,topic={0},producerIndex[{1}]={2},will use first one - expect only one";
                        Logger.Error(string.Format(s, this.topic, this.producerIndex, this.brokerConfList[this.producerIndex].ToString()));
                    }

                    this.PartitionsMetadata = topicMetaData.First().PartitionsMetadata;
                    foreach (var m in this.PartitionsMetadata)
                    {
                        if (this.partitionIndex < 0 || m.PartitionId == this.partitionIndex)
                        {
                            // avoid duplicates
                            if (!leaderBrokerIds.Contains(m.Leader.Id))
                            {
                                leaderBrokerIds.Add(m.Leader.Id);
                                this.leaders.Add(new BrokerConfiguration() { BrokerId = m.Leader.Id, Host = m.Leader.Host, Port = m.Leader.Port });
                            }

                            if (this.partitionIndex >= 0)
                            {
                                break;
                            }
                        }
                    }

                    if (this.leaders.Count == 0)
                    {
                        s = string.Format("no leader found,topic={0},partitionIndex={1},producerIndex[{1}]={2}", this.topic, this.partitionIndex, this.producerIndex, this.brokerConfList[this.producerIndex].ToString());
                        Logger.Error(s);
                        continue;
                    }

                    // simulate excepton in a debugger
                    //// bool t = false;
                    //// if (t)
                    //// {
                    ////     throw new ArgumentException("simulate exception in FindLeader");
                    //// }
                }
                catch (Exception e)
                {
                    s = "FindLeader hit exception,topic={0},producerIndex[{1}]={2},retryCount={3},maxRetry={4}";
                    Logger.Error(string.Format(s, this.topic, this.producerIndex, this.brokerConfList[this.producerIndex].ToString(), retryCount, maxRetry), e);
                    
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

        public string DumpLeader()
        {
            var sb = new StringBuilder();

            int i = 0;
            foreach (var l in this.leaders)
            {
                sb.AppendFormat("leader[{0}]:BrokerId={1},Host={2},Port={3}", i, l.BrokerId, l.Host, l.Port);
                sb.AppendLine();
                i++;
            }
            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }

        /// <summary>
        /// Push a message to Kafka
        /// </summary>
        /// <param name="msg"></param>
        [CLSCompliantAttribute(false)]
        public void PushMessage(IEnumerable<Message> msgs)
        {
            if (msgs == null || msgs.Count() == 0)
            {
                throw new ArgumentNullException("msg", "PushMessage received a null msg");
            }

            string s = string.Empty;
            // var messages = new List<Message>(1) { msg };
            int maxRetry = this.brokerConfList.Count;
            int retryCount = 0;
            bool success = false;

            while (!success)
            {
                // find the leader first for every re-try
                if (retryCount > 0)
                {
                    this.ClearLeader();
                }

                if (this.FindLeader() == 0)
                {
                    s = string.Format("PushMessege not found leader for topic {0}", this.topic);
                    //Logger.Error(s);
                    throw new ArgumentException(s);
                }

                //Logger.Debug(string.Format("KafkaClient.PushMessage,leader.First={0},retryCount={1},maxRetry={2}", leaders[0].ToString(), retryCount, maxRetry));
                try
                {
                    ProducerData<int, Message> data = null;
                    if (this.partitionIndex < 0)
                    {
                        data = new ProducerData<int, Message>(this.topic, msgs);
                    }
                    else
                    {
                        data = new ProducerData<int, Message>(this.topic, this.partitionIndex, msgs);
                    }

                    this.KafkaProducer.Send(data);

                    // simulate excepton in a debugger
                    //// bool t = false;
                    //// if (t)
                    //// {
                    ////     throw new ArgumentException("simulate exception in PushMessge");
                    //// }

                    success = true;
                }
                catch (Exception e)
                {
                    s = "PushMessage exception,leader={0},topic={1},retryCount={2},maxRetry={3},brokerConfigList.Count={4}";
                    Logger.Error(string.Format(s, leaders.Count == 0 ? "null" : leaders[0].ToString(), this.topic, retryCount, maxRetry, this.brokerConfList.Count), e);

                    if (retryCount >= maxRetry)
                    {
                        throw;
                    }
                }
                finally
                {
                    retryCount++;
                }
            } // end of while
        }

        public IList<byte[]> PullMessages(int fetchSizeInBytes = int.MaxValue)
        {
            int lastOffsetAdjustment = this.MaxMessagePerSet;
            return this.PullMessages(lastOffsetAdjustment, fetchSizeInBytes);
        }

        public IList<byte[]> PullMessages(int lastOffsetAdjustment, int fetchSizeInBytes)
        {
            List<byte[]> payload = null;
            long fetchOffset = -1111; // magic number indicating not initialized
            int maxRetry = this.brokerConfList.Count;
            int retryCount = 0;
            string s = string.Empty;
            bool success = false;

            while (!success)
            {
                // find the leader first for every retry
                if (retryCount > 0)
                {
                    this.ClearLeader();
                }

                if (this.FindLeader() == 0)
                {
                    s = string.Format("PullMessage not found leader for topic={0},retryCount={1},maxRetry={2}", this.topic, retryCount, maxRetry);
                    //Logger.Error(s);
                    throw new ArgumentException(s);
                }

                if (!this.AdjustOffset(lastOffsetAdjustment))
                {
                    // no valid offset, empty queue
                    break;
                }

                try
                {
                    var requestMap = new Dictionary<string, List<PartitionFetchInfo>>();
                    requestMap.Add(this.topic, new List<PartitionFetchInfo>() { new PartitionFetchInfo(this.partitionIndex, this.Offset, fetchSizeInBytes) });
                    fetchOffset = this.Offset;

                    s = "PullMessage BeforeFetch,topic={0},leader={1},partition={2},FetchOffset={3},retryCount={4},maxRetry={5}";
                    //Logger.Debug(string.Format(s, this.topic, leaders[0].ToString(), this.partitionIndex, fetchOffset, retryCount, maxRetry));

                    var response = this.KafkaConsumer.Fetch(new FetchRequest(
                        random.Next(int.MinValue, int.MaxValue),                        // correlation id
                        Assembly.GetExecutingAssembly().ManifestModule.ToString(),      // client id
                        this.ConsumerMaxWait,
                        0,
                        requestMap));

                    // simulate excepton in a debugger
                    //// bool t = false;
                    //// if (t)
                    //// {
                    ////     throw new ArgumentException("simulate exception in PullMessage,AfterFetch");
                    //// }

                    // var messages = response.MessageSet(this.topic, this.partitionIndex).ToList();
                    var messages = response.PartitionData(this.topic, this.partitionIndex).MessageSet.Messages.ToList();

                    this.OffsetType = KafkaOffsetType.Current;
                    s = "PullMessage AfterFetch,resultMessageCount={0},topic={1},leader={2},partition={3},FetechOffset={4},retryCount={5},maxRetry={6}";
                    //Logger.Debug(string.Format(s, null == messages ? "(null)" : messages.Count.ToString(), this.topic, leaders[0].ToString(), this.partitionIndex, fetchOffset, retryCount, maxRetry));

                    success = true;
                    if (null != messages && messages.Count > 0)
                    {
                        // prepare payload to be returned; adjust offset
                        payload = new List<byte[]>(messages.Count);
                        foreach (var v in messages)
                        {
                            payload.Add(v.Payload);
                            this.Offset = v.Offset + 1;
                        }
                    }
                }
                catch (Exception e)
                {
                    s = "PullMessage exception,fetchOffset={0},currentOffset={1},currentOffsetType={2},leader={3},topic={4},retryCount={5},maxRetry={6},maxWaitMilliseconds={7}";
                    Logger.Error(string.Format(s, fetchOffset, this.Offset, this.OffsetType, leaders.Count == 0 ? "null" : leaders[0].ToString(), this.topic, retryCount, maxRetry, this.ConsumerMaxWait), e);

                    if (retryCount >= maxRetry)
                    {
                        throw;
                    }
                }
                finally
                {
                    retryCount++;
                }
            } // end of while loop

            s = "PullMessage done,paylaoad.Count={0},fetchOffset={1},currentOffset={2},currentOffsetType={3},topic={4},retryCount={5},maxRetry={6},maxWaitMilliseconds={7}";
            //Logger.Debug(string.Format(s, payload == null ? -1 : payload.Count, fetchOffset, this.Offset, this.OffsetType, this.topic, retryCount, maxRetry, this.ConsumerMaxWait));

            return payload;
        }

        /// <summary>
        /// Adjust the offset based on OffsetType
        /// </summary>
        /// <param name="lastOffsetAdjustment">number of messages to adjust if OffsetType is "Last", for the caller to extract last message set</param>
        /// <returns>true if offset is set; false if offset is not found</returns>
        private bool AdjustOffset(int lastOffsetAdjustment)
        {
            if (this.OffsetType != KafkaOffsetType.Current)
            {
                long offsetEarliest = 0;
                offsetEarliest = KafkaClientHelper.GetOffset(this.KafkaConsumer, this.topic, this.partitionIndex, OffsetRequest.EarliestTime);
                switch (this.OffsetType)
                {
                    case KafkaOffsetType.Earliest:
                        this.Offset = offsetEarliest;
                        break;
                    case KafkaOffsetType.Last:
                    case KafkaOffsetType.Latest:
                        this.Offset = KafkaClientHelper.GetOffset(this.KafkaConsumer, this.topic, this.partitionIndex, OffsetRequest.LatestTime);
                        break;
                    default:
                        //Logger.Error(string.Format("invalid offsetType={0}", this.OffsetType));
                        throw new ArgumentOutOfRangeException(string.Format("offsetType={0}", this.OffsetType));
                }

                if (this.OffsetType == KafkaOffsetType.Last
                    || this.OffsetType == KafkaOffsetType.Latest)
                {
                    if (offsetEarliest == this.Offset)
                    {
                      //  Logger.Debug(string.Format("offsetEarliest==offsetLatest={0},queue empty,offset not found for {1}", this.Offset, this.OffsetType));
                        return false;
                    }

                    // adjust the offset for "Last", which should not be smaller than the earliest
                    this.Offset -= lastOffsetAdjustment;
                    if (this.Offset < offsetEarliest)
                    {
                        this.Offset = offsetEarliest;
                    }
                }
            }

            return true;
        }

        private void RoundRobinSyncProducer()
        {
            if (this.producerIndex == -1)
            {
                this.producerIndex = random.Next(0, this.brokerConfList.Count);
            }
            else
            {
                this.producerIndex = (this.producerIndex + 1) % this.brokerConfList.Count;
            }
        }

        private static long GetOffset(Consumer consumer, string topic, int partition, long offsetTime)
        {
            string s = string.Empty;
            bool success = false;
            long result = 0;

            if (consumer == null)
            {
                throw new ArgumentNullException("consumer");
            }

            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentNullException("topic");
            }

            try
            {
                var offsetRequestInfo = new Dictionary<string, List<PartitionOffsetRequestInfo>>();
                offsetRequestInfo.Add(topic, new List<PartitionOffsetRequestInfo>() { new PartitionOffsetRequestInfo(partition, offsetTime, 128) });
                var offsetRequest = new OffsetRequest(offsetRequestInfo);

                var offsetResponse = consumer.GetOffsetsBefore(offsetRequest);

                if (null == offsetResponse)
                {
                    s = string.Format("OffsetResponse for EarliestTime not found,topic={0}", topic);
                  //  Logger.Error(s);
                    throw new ArgumentException(s);
                }

                List<PartitionOffsetsResponse> partitionOffset = null;
                if (!offsetResponse.ResponseMap.TryGetValue(topic, out partitionOffset)
                    || partitionOffset == null
                    || partitionOffset.Count == 0)
                {
                    s = string.Format("OffsetResponse.ResponseMap for EarliestTime not found,topic={0}", topic);
                  //  Logger.Error(s);
                    throw new ArgumentException(s);
                }

                foreach (var v in partitionOffset)
                {
                    if (v.PartitionId == partition)
                    {
                        result = v.Offsets.First();
                        success = true;
                        break;
                    }
                }

                if (!success)
                {
                    s = string.Format("OffsetResponse.ResponseMap.Partition not found partition={0},topic={1}", partition, topic);
                  //  Logger.Error(s);
                    throw new ArgumentException(s);
                }
            }
            catch (Exception e)
            {
                Logger.Error(string.Format("GetOffset exception,partition={0},topic={1}", partition, topic), e);
                throw;
            }

            return result;
        }
    }
}