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

namespace Kafka.Client.Producers.Sync
{
    using Cfg;
    using Exceptions;
    using Messages;
    using Requests;
    using Responses;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Utils;

    /// <summary>
    /// Sends messages encapsulated in request to Kafka server synchronously
    /// </summary>
    public class SyncProducer : ISyncProducer
    {
        private readonly IKafkaConnection connection;
        private volatile bool disposed;

        /// <summary>
        /// Gets producer config
        /// </summary>
        public SyncProducerConfiguration Config { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncProducer"/> class.
        /// </summary>
        /// <param name="config">
        /// The producer config.
        /// </param>
        public SyncProducer(SyncProducerConfiguration config) : this(config, new KafkaConnection(
                config.Host,
                config.Port,
                config.BufferSize,
                config.SendTimeout,
                config.ReceiveTimeout,
                config.ReconnectInterval))
        {
        }

        public SyncProducer(SyncProducerConfiguration config, IKafkaConnection connection)
        {
            Guard.NotNull(config, "config");
            this.Config = config;
            this.connection = connection;
        }

        /// <summary>
        /// Sends request to Kafka server synchronously
        /// </summary>
        /// <param name="request">
        /// The request.
        /// </param>
        public ProducerResponse Send(ProducerRequest request)
        {
            this.EnsuresNotDisposed();

            foreach (var topicData in request.Data)
            {
                foreach (var partitionData in topicData.PartitionData)
                {
                    VerifyMessageSize(partitionData.MessageSet.Messages);
                }
            }

            return this.connection.Send(request);
        }

        public IEnumerable<TopicMetadata> Send(TopicMetadataRequest request)
        {
            return this.connection.Send(request);
        }

        /// <summary>
        /// Releases all unmanaged and managed resources
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
            if (this.connection != null)
            {
                this.connection.Dispose();
            }
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().Name);
            }
        }

        private void VerifyMessageSize(IEnumerable<Message> messages)
        {
            if (messages.Any(message => message.PayloadSize > Config.MaxMessageSize))
            {
                throw new MessageSizeTooLargeException();
            }
        }
    }
}
