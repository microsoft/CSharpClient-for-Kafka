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



namespace Kafka.Client.Cluster
{
    using Kafka.Client.ZooKeeperIntegration;
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Globalization;
    using System.Runtime.Serialization.Json;

    /// <summary>
    /// The set of active brokers in the cluster
    /// </summary>
    internal class Cluster
    {
        private readonly Dictionary<int, Broker> brokers;

        /// <summary>
        /// Initializes a new instance of the <see cref="Cluster"/> class.
        /// </summary>
        public Cluster()
        {
            this.brokers = new Dictionary<int, Broker>();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Cluster"/> class.
        /// </summary>
        /// <param name="zkClient">IZooKeeperClient object</param>
        public Cluster(IZooKeeperClient zkClient) : this()
        {
            var nodes = zkClient.GetChildrenParentMayNotExist(ZooKeeperClient.DefaultBrokerIdsPath);
            foreach (var node in nodes)
            {
                var brokerZkString = zkClient.ReadData<string>(ZooKeeperClient.DefaultBrokerIdsPath + "/" + node);
                Broker broker = this.CreateBroker(node, brokerZkString);
                brokers[broker.Id] = broker;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Cluster"/> class.
        /// </summary>
        /// <param name="brokers">
        /// The set of active brokers.
        /// </param>
        public Cluster(IEnumerable<Broker> brokers) : this()
        {
            foreach (var broker in brokers)
            {
                this.brokers.Add(broker.Id, broker);
            }
        }

        /// <summary>
        /// Gets broker with given ID
        /// </summary>
        /// <param name="id">
        /// The broker ID.
        /// </param>
        /// <returns>
        /// The broker with given ID
        /// </returns>
        public Broker GetBroker(int id)
        {
            if (this.brokers.ContainsKey(id))
            {
                return this.brokers[id];
            }

            return null;
        }

        /// <summary>
        /// Gets brokers collection
        /// </summary>
        public Dictionary<int, Broker> Brokers
        {
            get
            {
                return this.brokers;
            }
        }
        
        /// <summary>
        /// Creates a new Broker object out of a BrokerInfoString
        /// </summary>
        /// <param name="node">node string</param>
        /// <param name="brokerInfoString">the BrokerInfoString</param>
        /// <returns>Broker object</returns>
        private Broker CreateBroker(string node, string brokerInfoString)
        {
            int id;
            if (int.TryParse(node, NumberStyles.Integer, CultureInfo.InvariantCulture, out id))
            {
                return Broker.CreateBroker(id, brokerInfoString);
            }
            
            throw new ArgumentException(String.Format(CultureInfo.CurrentCulture, "{0} is not a valid integer", node));
        }
    }
}
