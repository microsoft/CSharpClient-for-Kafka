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
namespace Kafka.Client.Cfg
{
    using Kafka.Client.Requests;
    using System.Configuration;
    using System.Xml.Linq;


    public class ConsumerConfigurationSection : ConfigurationSection
    {
        [ConfigurationProperty("numberOfTries", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultNumberOfTries)]
        public short NumberOfTries
        {
            get
            {
                return (short)this["numberOfTries"];
            }
        }

        [ConfigurationProperty("groupId", IsRequired = true)]
        public string GroupId
        {
            get
            {
                return (string)this["groupId"];
            }
        }

        [ConfigurationProperty("timeout", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultTimeout)]
        public int Timeout
        {
            get
            {
                return (int)this["timeout"];
            }
        }

        [ConfigurationProperty("autoOffsetReset", IsRequired = false, DefaultValue = OffsetRequest.SmallestTime)]
        public string AutoOffsetReset
        {
            get
            {
                return (string)this["autoOffsetReset"];
            }
        }

        [ConfigurationProperty("consumerId", IsRequired = false, DefaultValue = null)]
        public string ConsumerId
        {
            get
            {
                return (string)this["consumerId"];
            }
        }

        [ConfigurationProperty("autoCommit", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultAutoCommit)]
        public bool AutoCommit
        {
            get
            {
                return (bool)this["autoCommit"];
            }
        }

        [ConfigurationProperty("autoCommitInterval", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultAutoCommitInterval)]
        public int AutoCommitInterval
        {
            get
            {
                return (int)this["autoCommitInterval"];
            }
        }

        [ConfigurationProperty("fetchSize", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultFetchSize)]
        public int FetchSize
        {
            get
            {
                return (int)this["fetchSize"];
            }
        }

        [ConfigurationProperty("backOffIncrement", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultBackOffIncrement)]
        public int BackOffIncrement
        {
            get
            {
                return (int)this["backOffIncrement"];
            }
        }

        [ConfigurationProperty("sendTimeout", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultSocketTimeout)]
        public int SendTimeout
        {
            get
            {
                return (int)this["sendTimeout"];
            }
        }

        [ConfigurationProperty("receiveTimeout", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultSocketTimeout)]
        public int ReceiveTimeout
        {
            get
            {
                return (int)this["receiveTimeout"];
            }
        }

        [ConfigurationProperty("bufferSize", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultSocketTimeout)]
        public int BufferSize
        {
            get
            {
                return (int)this["bufferSize"];
            }
        }

        [ConfigurationProperty("maxFetchBufferLength", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultMaxFetchBufferLength)]
        public int MaxFetchBufferLength
        {
            get
            {
                return (int)this["maxFetchBufferLength"];
            }
        }

        [ConfigurationProperty("shutdownTimeout", IsRequired = false, DefaultValue = ConsumerConfiguration.DefaultShutdownTimeout)]
        public int ShutdownTimeout
        {
            get
            {
                return (int)this["shutdownTimeout"];
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

        [ConfigurationProperty("broker", IsRequired = false)]
        public BrokerConfigurationElement Broker
        {
            get
            {
                return (BrokerConfigurationElement)this["broker"];
            }
        }

        public static ConsumerConfigurationSection FromXml(XElement element)
        {
            var section = new ConsumerConfigurationSection();
            section.DeserializeSection(element.CreateReader());
            return section;
        }
    }
}
