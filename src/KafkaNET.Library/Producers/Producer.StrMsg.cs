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
    using Kafka.Client.Messages;
    using Kafka.Client.Producers.Partitioning;

    /// <summary>
    /// High-level Producer API that exposes all the producer functionality to the client 
    /// using <see cref="System.String" /> as type of key and <see cref="Message" /> as type of data
    /// </summary>
    public class Producer : Producer<string, Message>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Producer"/> class.
        /// </summary>
        /// <param name="config">The config object.</param>
        /// <remarks>
        /// Can be used when all config parameters will be specified through the config object
        /// and will be instantiated via reflection
        /// </remarks>
        public Producer(ProducerConfiguration config)
            : base(config)
        {
        }
    }
}
