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

using System.Collections.Generic;
using Kafka.Client.Messages;

namespace Kafka.Client.Cfg
{
    using System.Configuration;
    using Kafka.Client.Producers;
    using System.Xml.Linq;
    using System.Linq;

    public class ProducerConfigurationSection : ConfigurationSection
    {
        [ConfigurationProperty(
            "type",
            DefaultValue = ProducerConfiguration.DefaultProducerType,
            IsRequired = false)]
        public ProducerTypes ProducerType
        {
            get
            {
                return (ProducerTypes)this["type"];
            }
        }

        [ConfigurationProperty(
            "bufferSize",
            DefaultValue = SyncProducerConfiguration.DefaultBufferSize,
            IsRequired = false)]
        public int BufferSize
        {
            get
            {
                return (int)this["bufferSize"];
            }
        }

        [ConfigurationProperty(
            "connectionTimeout",
            DefaultValue = SyncProducerConfiguration.DefaultConnectTimeout,
            IsRequired = false)]
        public int ConnectionTimeout
        {
            get
            {
                return (int)this["connectionTimeout"];
            }
        }

        [ConfigurationProperty(
            "socketTimeout",
            DefaultValue = SyncProducerConfiguration.DefaultSocketTimeout,
            IsRequired = false)]
        public int SocketTimeout
        {
            get
            {
                return (int)this["socketTimeout"];
            }
        }

        [ConfigurationProperty(
            "maxMessageSize",
            DefaultValue = SyncProducerConfiguration.DefaultMaxMessageSize,
            IsRequired = false)]
        public int MaxMessageSize
        {
            get
            {
                return (int)this["maxMessageSize"];
            }
        }

        [ConfigurationProperty("brokers", IsRequired = false, IsDefaultCollection = true)]
        [ConfigurationCollection(typeof(BrokerConfigurationElementCollection),
            AddItemName = "add",
            ClearItemsName = "clear",
            RemoveItemName = "remove")]
        public BrokerConfigurationElementCollection Brokers
        {
            get
            {
                return (BrokerConfigurationElementCollection)this["brokers"];
            }
        }

        [ConfigurationProperty("zookeeper", IsRequired = false, DefaultValue = null)]
        public ZooKeeperConfigurationElement ZooKeeperServers
        {
            get 
            { 
                 return (ZooKeeperConfigurationElement)this["zookeeper"];
            }
        }

        [ConfigurationProperty(
            "serializer",
            DefaultValue = ProducerConfiguration.DefaultSerializer,
            IsRequired = false)]
        public string Serializer
        {
            get
            {
                return (string)this["serializer"];
            }
        }

        [ConfigurationProperty(
            "partitioner",
            DefaultValue = ProducerConfiguration.DefaultPartitioner,
            IsRequired = false)]
        public string Partitioner
        {
            get
            {
                return (string)this["partitioner"];
            }
        }

        [ConfigurationProperty(
            "compressionCodec",
            DefaultValue = CompressionCodecs.DefaultCompressionCodec,
            IsRequired = false)]
        public CompressionCodecs CompressionCodec
        {
            get
            {
                return Messages.CompressionCodec.GetCompressionCodec((int)this["compressionCodec"]);
            }
        }

        [ConfigurationProperty(
            "compressedTopics",
            DefaultValue = null,
            IsRequired = false)]
        public List<string> CompressedTopics
        {
            get
            {
                if (string.IsNullOrEmpty(((string)this["compressedTopics"])))
                { return new List<string>(); }
                else
                {
                    return
                        new List<string>(
                            ((string) this["compressedTopics"]).Split(',').Where(x => !string.IsNullOrEmpty(x)));
                }
            }
        }

        [ConfigurationProperty(
            "producerRetries",
            DefaultValue = 3,
            IsRequired = false)]
        public int ProducerRetries
        {
            get
            {
                return (int)this["producerRetries"];
            }
        }

        [ConfigurationProperty(
            "producerRetryExpoBackoffMinMs",
            DefaultValue = 500,
            IsRequired = false)]
        public int ProducerRetryExpoBackoffMinMs
        {
            get
            {
                return (int)this["producerRetryExpoBackoffMinMs"];
            }
        }

        [ConfigurationProperty(
            "producerRetryExpoBackoffMaxMs",
            DefaultValue = 500,
            IsRequired = false)]
        public int ProducerRetryExpoBackoffMaxMs
        {
            get
            {
                return (int)this["producerRetryExpoBackoffMaxMs"];
            }
        }

        [ConfigurationProperty(
            "correlationId",
            DefaultValue = -1,
            IsRequired = false)]
        public int CorrelationId
        {
            get
            {
                return (int)this["correlationId"];
            }
        }

        [ConfigurationProperty(
            "clientId",
            DefaultValue = "",
            IsRequired = false)]
        public string ClientId
        {
            get
            {
                return (string)this["clientId"];
            }
        }

        [ConfigurationProperty(
            "requiredAcks",
            DefaultValue = (short)0,
            IsRequired = false)]
        public short RequiredAcks
        {
            get
            {
                return (short)this["requiredAcks"];
            }
        }

        [ConfigurationProperty(
            "ackTimeout",
            DefaultValue = 1,
            IsRequired = false)]
        public int AckTimeout
        {
            get
            {
                return (int)this["ackTimeout"];
            }
        }

        [ConfigurationProperty(
            "queueTime",
            DefaultValue = 5000,
            IsRequired = false)]
        public int QueueTime
        {
            get
            {
                return (int)this["queueTime"];
            }
        }

        [ConfigurationProperty(
            "queueSize",
            DefaultValue = 10000,
            IsRequired = false)]
        public int QueueSize
        {
            get
            {
                return (int)this["queueSize"];
            }
        }

        [ConfigurationProperty(
            "batchSize",
            DefaultValue = 200,
            IsRequired = false)]
        public int BatchSize
        {
            get
            {
                return (int)this["batchSize"];
            }
        }

        [ConfigurationProperty(
            "enqueueTimeoutMs",
            DefaultValue = 0,
            IsRequired = false)]
        public int EnqueueTimeoutMs
        {
            get
            {
                return (int)this["enqueueTimeoutMs"];
            }
        }

        public static ProducerConfigurationSection FromXml(XElement xml)
        {
            var config = new ProducerConfigurationSection();
            config.DeserializeSection(xml.CreateReader());
            return config;
        }

    }
}
