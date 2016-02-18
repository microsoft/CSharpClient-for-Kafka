// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Cfg
{
    using Kafka.Client.Consumers;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Contains main fields of ProducerConfiguration and ConsumerConfiguration
    /// </summary>
    public class KafkaSimpleManagerConfiguration
    {
        #region Default Value
        public const int DefaultFetchSize = 11 * 1024 * 1024;
        public const int DefaultBufferSize = 11 * 1024 * 1024;  
        #endregion
        public KafkaSimpleManagerConfiguration()
        {
            MaxMessageSize = SyncProducerConfiguration.DefaultMaxMessageSize;
            BufferSize = DefaultBufferSize;
            FetchSize = DefaultFetchSize;
            MaxWaitTime = 0;
            MinWaitBytes = 0;         
            AckTimeout = SyncProducerConfiguration.DefaultAckTimeout;
            PartitionerClass = string.Empty;
            //TopicMetaDataRefreshIntervalMS = ProducerConfiguration.DefaultTopicMetaDataRefreshIntervalMS;
        }
        public void Verify()
        {
            if (ZookeeperConfig == null )
            {
                throw new ArgumentException(" zookeeper  should be provided");
            }
        }

        public string Zookeeper { set { this.ZookeeperConfig = KafkaClientHelperUtils.ToZookeeperConfig(value); } }
        public ZooKeeperConfiguration ZookeeperConfig { get; private set; }

        /// <summary>
        ///  Currently didn't use yet. topic.metadata.refresh.interval.ms
        /// </summary>
        //public int TopicMetaDataRefreshIntervalMS { get; set; }

        #region ProducerConfiguration only
        /// <summary>
        /// ProducerConfiguration  - Max size of one message, used for verify size and throw exception.
        /// Default: 1 M
        /// </summary>
        public int MaxMessageSize { get; set; }

        /// <summary>
        /// ProducerConfiguration ==== AckTimeout  Ack Timeout. Default to 300 milliseconds
        /// </summary>
        public int AckTimeout { get; set; }
        /// <summary>
        /// ProducerConfiguration ==== PartitionerClass. Default is empty.
        /// If this value is null/empty,   the kafkaSimpleManager will create on producer for each partition.
        /// Else if this value is one partitioner class full name, the kafkaSimpleManager will only create one producer.  And that producer has internal syncproducer pool to partition data by the class and send data.
        /// </summary>
        public string PartitionerClass { get; set; }
        #endregion

        #region Produce and Consume
        /// <summary>
        /// ProducerConfiguration - ConsumerConfiguration - The socket recieve / send buffer size. in bytes.
        /// Map to socket.receive.buffer.bytes in java api.
        /// Default value 11 * 1024 * 1024
        /// java version original default  value: 64 *1024
        /// </summary>
        public int BufferSize { get; set; }
        /// <summary>
        /// Log is verbose or not
        /// </summary>
        public bool Verbose { get; set; }
        #endregion

        #region Consume only
        /// <summary>
        /// Consume API ==== FetchRequest - the number of byes of messages to attempt to fetch. 
        /// map to fetch.message.max.bytes of java version.
        /// Finally it call FileChannle.position(long newPosition)  and got to native call position0(FileDescriptor fd, long offset)
        /// Default value: 11 * 1024*1024
        /// </summary>
        public int FetchSize { get; set; }

        /// <summary>
        /// Consume API ==== FetchRequest MaxWait
        /// http://kafka.apache.org/documentation.html#simpleconsumerapi, 
        /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchRequest
        /// fetch.wait.max.ms, 100ms, The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes
        /// Observation: if message is pushed to Kafka during the wait, somehow the client is still blocked on the wait, 
        /// Recommend to set this value based on the understanding of the message arrival rate in Kafka. 
        /// https://kafka.apache.org/08/configuration.html
        /// Default value  to 0 ms.
        /// </summary>
        public int MaxWaitTime { get; set; }
        /// <summary>
        /// Consume API ==== FetchRequest MinBytes
        /// Use together with previous one MaxWaitTime
        /// fetch.min.bytes
        /// default value 0 
        /// </summary>
        public int MinWaitBytes { get; set; }
        #endregion
    }
}
