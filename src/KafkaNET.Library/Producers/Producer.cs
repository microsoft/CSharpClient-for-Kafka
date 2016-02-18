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

namespace Kafka.Client.Producers
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    /// <summary>
    /// High-level Producer API that exposes all the producer functionality to the client
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TData">The type of the data.</typeparam>
    /// <remarks>
    /// Provides serialization of data through a user-specified encoder, zookeeper based automatic broker discovery
    /// and software load balancing through an optionally user-specified partitioner
    /// </remarks>
    public class Producer<TKey, TData> : KafkaClientBase, IProducer<TKey, TData>
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger("Producer");

        private readonly ICallbackHandler<TKey, TData> callbackHandler;
        private volatile bool disposed;
        private readonly object shuttingDownLock = new object();
        private readonly IDictionary<string, TopicMetadata> topicPartitionInfo = new Dictionary<string, TopicMetadata>();
        private readonly IDictionary<string, DateTime> topicPartitionInfoLastUpdateTime = new Dictionary<string, DateTime>();
        private SyncProducerPool syncProducerPool;

        public Producer(ICallbackHandler<TKey, TData> callbackHandler)
        {
            this.callbackHandler = callbackHandler;
        }

        public Producer(ProducerConfiguration config)
        {
            this.Config = config;

            syncProducerPool = new SyncProducerPool(config);
            this.callbackHandler = new DefaultCallbackHandler<TKey, TData>(config,
                ReflectionHelper.Instantiate<IPartitioner<TKey>>(config.PartitionerClass),
                ReflectionHelper.Instantiate<IEncoder<TData>>(config.SerializerClass),
                new BrokerPartitionInfo(syncProducerPool, topicPartitionInfo, topicPartitionInfoLastUpdateTime, Config.TopicMetaDataRefreshIntervalMS, syncProducerPool.zkClient),
                syncProducerPool);
        }

        public ProducerConfiguration Config { get; private set; }

        /// <summary>
        /// Sends the data to a multiple topics, partitioned by key
        /// </summary>
        /// <param name="data">The producer data objects that encapsulate the topic, key and message data.</param>
        public void Send(IEnumerable<ProducerData<TKey, TData>> data)
        {
            Guard.NotNull(data, "data");
            Guard.CheckBool(data.Any(), true, "data.Any()");

            this.EnsuresNotDisposed();

            this.callbackHandler.Handle(data);
        }

        /// <summary>
        /// Sends the data to a single topic, partitioned by key, using either the
        /// synchronous or the asynchronous producer.
        /// </summary>
        /// <param name="data">The producer data object that encapsulates the topic, key and message data.</param>
        public void Send(ProducerData<TKey, TData> data)
        {
            Guard.NotNull(data, "data");
            Guard.NotNullNorEmpty(data.Topic, "data.Topic");
            Guard.NotNull(data.Data, "data.Data");
            Guard.CheckBool(data.Data.Any(), true, "data.Data.Any()");

            this.EnsuresNotDisposed();

            this.Send(new List<ProducerData<TKey, TData>> { data });
        }

        public override string ToString()
        {
            if (this.Config == null)
                return "Producer: Config is null.";
            if (this.syncProducerPool == null)
                return "Producer: syncProducerPool is null.";
            return "Producer: " + this.syncProducerPool.ToString();
        }
        protected override void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (this.disposed)
            {
                return;
            }

            lock (this.shuttingDownLock)
            {
                if (this.disposed)
                {
                    return;
                }

                this.disposed = true;
            }

            try
            {
                if (this.callbackHandler != null)
                {
                    this.callbackHandler.Dispose();
                }
            }
            catch (Exception exc)
            {
                Logger.Error(exc.FormatException());
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
    }
}
