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
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// The base for all classes that represents pool of producers used by high-level API
    /// </summary>
    public class SyncProducerPool : ISyncProducerPool
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(SyncProducerPool));

        /// <summary>
        /// BrokerID  -->  SyncProducer
        /// </summary>
        internal readonly ConcurrentDictionary<int, SyncProducerWrapper> syncProducers;
        protected ProducerConfiguration Config { get; private set; }
        internal readonly ZooKeeperClient zkClient;
        private readonly ThreadSafeRandom random = new ThreadSafeRandom();

        public SyncProducerPool(ProducerConfiguration config)
        {
            this.syncProducers = new ConcurrentDictionary<int, SyncProducerWrapper>();
            this.Config = config;

            if (config.ZooKeeper != null)
            {
                this.zkClient = new ZooKeeperClient(config.ZooKeeper.ZkConnect, config.ZooKeeper.ZkSessionTimeoutMs,
                                                   ZooKeeperStringSerializer.Serializer);
                this.zkClient.Connect();
            }

            this.AddProducers(config);
        }

        public SyncProducerPool(ProducerConfiguration config, List<ISyncProducer> producers)
        {
            this.syncProducers = new ConcurrentDictionary<int, SyncProducerWrapper>();
            this.Config = config;

            if (config.ZooKeeper != null)
            {
                this.zkClient = new ZooKeeperClient(config.ZooKeeper.ZkConnect, config.ZooKeeper.ZkSessionTimeoutMs,
                                                   ZooKeeperStringSerializer.Serializer);
                this.zkClient.Connect();
            }

            if (producers != null && producers.Any())
            {
                producers.ForEach(x => this.syncProducers.TryAdd(x.Config.BrokerId, new SyncProducerWrapper(x, config.SyncProducerOfOneBroker)));
            }
        }

        protected bool Disposed { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SyncProducerPool:");
            var configuredBrokers = Config.Brokers.Select(x => new Broker(x.BrokerId, x.Host, x.Port)).ToArray();
            sb.AppendFormat("\tSeed brokers:");
            sb.Append(string.Join(",", configuredBrokers.Select(r => r.ToString()).ToArray()));

            if (Config.ZooKeeper != null)
                sb.AppendFormat("\t Broker zookeeper: {0} \t", this.Config.ZooKeeper.ZkConnect);

            sb.Append(string.Join(",", this.syncProducers.Select(r => string.Format("BrokerID:{0} syncProducerCount:{1} ", r.Key, r.Value.Producers.Count())).ToArray()));
            return sb.ToString();
        }
        public void AddProducer(Broker broker)
        {
            var syncProducerConfig = new SyncProducerConfiguration(this.Config, broker.Id, broker.Host, broker.Port);
            var producerWrapper = new SyncProducerWrapper(syncProducerConfig, this.Config.SyncProducerOfOneBroker);
            Logger.DebugFormat("Creating sync producer for broker id = {0} at {1}:{2} SyncProducerOfOneBroker:{3}", broker.Id, broker.Host, broker.Port, this.Config.SyncProducerOfOneBroker);
            this.syncProducers.TryAdd(broker.Id, producerWrapper);
        }

        public void AddProducers(ProducerConfiguration config)
        {
            var configuredBrokers = config.Brokers.Select(x => new Broker(x.BrokerId, x.Host, x.Port));
            if (configuredBrokers.Any())
            {
                configuredBrokers.ForEach(this.AddProducer);
            }
            else if (this.zkClient != null)
            {
                Logger.DebugFormat("Connecting to {0} for creating sync producers for all brokers in the cluster",
                                   config.ZooKeeper.ZkConnect);
                var brokers = ZkUtils.GetAllBrokersInCluster(this.zkClient);
                brokers.ForEach(this.AddProducer);
            }
            else
            {
                throw new IllegalStateException("No producers found from configuration and zk not setup.");
            }
        }
        public int Count()
        {
            return this.syncProducers.Count;
        }

        public List<ISyncProducer> GetShuffledProducers()
        {
            return this.syncProducers.Values.OrderBy(a => random.Next()).ToList().Select(r => r.GetProducer()).ToList();
        }

        public List<ISyncProducer> GetProducers()
        {
            return this.syncProducers.OrderBy(a => a.Key).Select(r => r.Value).ToList().Select(r => r.GetProducer()).ToList();
        }

        public ISyncProducer GetProducer(int brokerId)
        {
            SyncProducerWrapper producerWrapper;
            syncProducers.TryGetValue(brokerId, out producerWrapper);
            if (producerWrapper == null)
            {
                throw new UnavailableProducerException(
                    string.Format("Sync producer for broker id {0} does not exist", brokerId));
            }

            return producerWrapper.GetProducer();
        }

        /// <summary>
        /// Releases all unmanaged and managed resources
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            if (this.Disposed)
            {
                return;
            }

            this.Disposed = true;
            this.syncProducers.ForEach(x => x.Value.Dispose());
            if (this.zkClient != null)
            {
                this.zkClient.Dispose();
            }
        }

        internal class SyncProducerWrapper
        {
            public Queue<ISyncProducer> Producers;

            private readonly object _lock = new object();

            public SyncProducerWrapper(SyncProducerConfiguration syncProducerConfig, int count)
            {
                Producers = new Queue<ISyncProducer>(count);
                for (int i = 0; i < count; i++)
                {
                    //TODO: if can't create , should retry later. should not block
                    Producers.Enqueue(new SyncProducer(syncProducerConfig));
                }
            }

            protected bool Disposed { get; set; }

            public SyncProducerWrapper(ISyncProducer syncProducer, int count)
            {
                Producers = new Queue<ISyncProducer>(count);
                for (int i = 0; i < count; i++)
                {
                    Producers.Enqueue(syncProducer);
                }
            }

            public ISyncProducer GetProducer()
            {
                //round-robin instead of random peek, to avoid collision of producers.
                ISyncProducer producer = null;
                lock (_lock)
                {
                    producer = Producers.Peek();
                    Producers.Dequeue();
                    Producers.Enqueue(producer);
                }
                return producer;
            }

            public void Dispose()
            {
                this.Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected void Dispose(bool disposing)
            {
                if (!disposing)
                {
                    return;
                }

                if (this.Disposed)
                {
                    return;
                }

                this.Disposed = true;
                this.Producers.ForEach(x => x.Dispose());
            }
        }
    }
}
