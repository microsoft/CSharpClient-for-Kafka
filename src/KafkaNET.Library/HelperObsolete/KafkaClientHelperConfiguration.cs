using Kafka.Client.Cfg;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Client.Consumers
{
    /// <summary>
    /// This class should be obsolete.
    /// Please use KafkaSimpleManger and KafkaSimpleManagerConfiguration
    /// </summary>
    public class KafkaClientHelperConfiguration : KafkaSimpleManagerConfiguration
    {
        // Observed Zookeeper reaching max number of connections in the sandbox. Suspect KafakClient contributed to the issue. 
        // Instead of every 30 minutes to refresh the broker list (30 * 60 * 1000 in millisecond), changed the interval to 68 years
        // as a workaroud for now.
        public const int DefaultBrokerListFromZookeeperRefreshIntervalMilliSecond = int.MaxValue;
        public const int DefaultMaxMessagePerSet = 100;
        public const int DefaultTimeoutMilliSecond = Kafka.Client.Cfg.SyncProducerConfiguration.DefaultSendTimeout;
        public const int DefaultMaxMessageSizeByte = 10 * 1024 * 1024;
        public const int DefaultBufferExtraByte = 1024 * 1024;  
        public KafkaClientHelperConfiguration():base()
        {
            Offset = 0;
            OffsetType = KafkaOffsetType.Earliest;
            BrokerListFromZookeeperRefreshIntervalMS = DefaultBrokerListFromZookeeperRefreshIntervalMilliSecond;
            MaxMessagePerSet = DefaultMaxMessagePerSet;
            Timeout = DefaultTimeoutMilliSecond;
            MaxMessageSize = DefaultMaxMessageSizeByte;
            BufferExtra = DefaultBufferExtraByte;
            FetchSize = DefaultFetchSize;
        }
        public string KafkaBrokerList { get; set; }

        /// <summary>
        /// Either Leader, KafkaBrokerList or Zookeeper must be provided to KafkaClientHelper; when more than one option is provided, KafkaClientHelper honors 
        /// the option in the order of Leader, KafkaBrokerList and Zookeeper at the last.
        ///     (1) Zookeeper option is recommended, so KafkaClientHelper can dynamically adjust the broker list from Zookeeper at runtime;
        ///     (2) Leader option is provided mainly for trouble-shooting purpose; it requires a leaderParition to be specified as well
        ///     (3) KafkaBrokerList option is provided mainly for backward compatability.
        /// </summary>
        public string Leader { set { this.LeaderConfig = KafkaClientHelperUtils.ToBrokerConfig(value); } }
        public int LeaderPartition { get; set; }
        public BrokerConfiguration LeaderConfig { get; set; }
        /// <summary>
        /// When KafkaBrokerList or Zookeeper is provided, KafkaClient will retrieve TotalNumPartitions from topicMetaData and ignore this property.
        /// This property is only used if the caller intends to test push or pull from a specific leader for a given parition.
        /// </summary>
        public int TotalNumPartitions { get; set; }


        public long Offset { get; set; }

        /// <summary>
        /// OffsetTimestamp contains Unix time in milliseconds. 
        /// Note: OffsetType must be set to Timestamp, for KafkaClientHelper to pick up OffsetTimestamp
        /// </summary>
        public long OffsetTimestamp { get; set; }

        public KafkaOffsetType OffsetType { get; set; }

        /// <summary>
        /// When KafkaClient retrives the list of brokers from Zookeeper, the list needs to be refreshed in case new brokers are added. 
        /// Default to 30 minutes
        /// </summary>
        public int BrokerListFromZookeeperRefreshIntervalMS { get; set; }

        /// <summary>
        /// When a caller saves a blob of across more than one Kafka message and tries to retrieve the blob with OffsetType == Last,
        /// KafkaClientHelper extracts this number of Kafka Messages counting backwards from Offset == Latest.
        /// Default to 20.
        /// </summary>
        public int MaxMessagePerSet { get; set; }
        /// <summary>
        /// Connection Time. Default to  5 seconds.
        /// </summary>
        public int Timeout { get; set; }

        /// <summary>
        /// Wait time in millseconds before next retry. Default to 0.
        /// </summary>
        public int MaxRetryWaitTime { get; set; }
        ///// <summary>
        ///// ConsumerConfiguration.BufferSize = .MaxMessageSize + .BufferExtra
        ///// ConsumerConfiguration.FetchSize  =  .MaxMessageSize + .BufferExtra
        ///// Maximum bytes in one Kafka Message. AdsLSE SCP.Net customized Kafka MaxFetchSize to 20Mbytes. 
        ///// Default to 10Mbytes to leave margin of safety 
        ///// </summary>
        //public int MaxMessageSize { get; set; }

        /// <summary>
        /// ConsumerConfiguration.BufferSize = .MaxMessageSize + .BufferExtra
        /// ConsumerConfiguration.FetchSize  =  .MaxMessageSize + .BufferExtra
        /// MaxMessageSize + BufferExtra is passed to Kafka pull request as the FetchSize.
        /// Default to 1Mbytes
        /// </summary>
        public int BufferExtra { get; set; }

    }
}
