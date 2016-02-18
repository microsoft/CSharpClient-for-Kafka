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

namespace Kafka.Client.Requests
{
    using Kafka.Client.Consumers;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class FetchRequestBuilder
    {
        private int correlationId = -1;
        private string clientId = string.Empty;
        private int maxWait = -1;
        private int minBytes = -1;
        private readonly Dictionary<string, List<PartitionFetchInfo>> requestMap = new Dictionary<string, List<PartitionFetchInfo>>();

        public FetchRequestBuilder AddFetch(string topic, int partition, long offset, int fetchSize)
        {
            var fetchInfo = new PartitionFetchInfo(partition, offset, fetchSize);
            if (!this.requestMap.ContainsKey(topic))
            {
                var item = new List<PartitionFetchInfo> { fetchInfo };
                this.requestMap.Add(topic, item);
            }
            else
            {
                this.requestMap[topic].Add(fetchInfo);
            }
            return this;
        }

        public FetchRequestBuilder CorrelationId(int correlationId)
        {
            this.correlationId = correlationId;
            return this;
        }

        public FetchRequestBuilder ClientId(string clientId)
        {
            this.clientId = clientId;
            return this;
        }

        public FetchRequestBuilder MaxWait(int maxWait)
        {
            this.maxWait = maxWait;
            return this;
        }

        public FetchRequestBuilder MinBytes(int minBytes)
        {
            this.minBytes = minBytes;
            return this;
        }

        public FetchRequest Build()
        {
            return new FetchRequest(correlationId, clientId, maxWait, minBytes, requestMap);
        }
    }
}
