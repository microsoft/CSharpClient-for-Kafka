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
    using Cluster;
    using Kafka.Client.Cfg;
    using Kafka.Client.Producers.Sync;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Pool of producers used by producer high-level API
    /// </summary>
    public interface ISyncProducerPool : IDisposable
    {
        /// <summary>
        /// Add a new producer, either synchronous or asynchronous, to the pool
        /// </summary>
        /// <param name="broker">The broker informations.</param>
        void AddProducer(Broker broker);

        List<ISyncProducer> GetShuffledProducers();
        void AddProducers(ProducerConfiguration producerConfig);
        ISyncProducer GetProducer(int brokerId);
    }
}
