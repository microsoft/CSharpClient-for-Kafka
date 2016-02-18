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
    using Kafka.Client.Utils;

    public class SyncProducerConfiguration : ISyncProducerConfigShared
    {
        public const int DefaultBufferSize = 100 * 1024;

        public const int DefaultConnectTimeout = 5 * 1000;

        public const int DefaultReceiveTimeout = 5 * 1000;

        public const int DefaultSendTimeout = 5 * 1000;

        public const int DefaultReconnectInterval = 30 * 1000;

        public const int DefaultMaxMessageSize = 1024 * 1024;

        public const int DefaultCorrelationId = -1;

        public const string DefaultClientId = "";

        public const short DefaultRequiredAcks = 0;

        public const int DefaultAckTimeout = 300;

        public SyncProducerConfiguration()
        {
            this.BufferSize = DefaultBufferSize;
            this.ConnectTimeout = DefaultConnectTimeout;
            this.MaxMessageSize = DefaultMaxMessageSize;
            this.CorrelationId = DefaultCorrelationId;
            this.ClientId = DefaultClientId;
            this.RequiredAcks = DefaultRequiredAcks;
            this.AckTimeout = DefaultAckTimeout;
            this.ReconnectInterval = DefaultReconnectInterval;
            this.ReceiveTimeout = DefaultReceiveTimeout;
            this.SendTimeout = DefaultSendTimeout;
        }

        public SyncProducerConfiguration(ProducerConfiguration config, int id, string host, int port) 
        {
            Guard.NotNull(config, "config");

            this.Host = host;
            this.Port = port;
            this.BrokerId = id;
            this.BufferSize = config.BufferSize;
            this.ConnectTimeout = config.ConnectTimeout;
            this.MaxMessageSize = config.MaxMessageSize;
            this.ReconnectInterval = config.ReconnectInterval;
            this.ReceiveTimeout = config.ReceiveTimeout;
            this.SendTimeout = config.SendTimeout;
            this.ClientId = config.ClientId;
            this.CorrelationId = DefaultCorrelationId;
            this.RequiredAcks = config.RequiredAcks;
            this.AckTimeout = config.AckTimeout;
        }

        public int BufferSize { get; set; }

        public int ConnectTimeout { get; set; }

        public int KeepAliveTime { get; set; }

        public int KeepAliveInterval { get; set; }

        public int ReceiveTimeout { get; set; }
        
        public int SendTimeout { get; set; }

        public int MaxMessageSize { get; set; }

        public string Host { get; set; }

        public int Port { get; set; }

        public int BrokerId { get; set; }

        public int CorrelationId{ get; set; }

        public string ClientId{ get; set; }

        public short RequiredAcks{ get; set; }

        public int AckTimeout { get; set; }

        public int ReconnectInterval { get; set; }
    }
}
