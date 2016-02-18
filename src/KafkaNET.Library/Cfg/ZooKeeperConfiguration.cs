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
    public class ZooKeeperConfiguration
    {
        public const int DefaultSessionTimeout = 6000;

        public const int DefaultConnectionTimeout = 6000;

        public const int DefaultSyncTime = 2000;

        public ZooKeeperConfiguration()
            : this(null, DefaultSessionTimeout, DefaultConnectionTimeout, DefaultSyncTime)
        {
        }

        public ZooKeeperConfiguration(string zkconnect, int zksessionTimeoutMs, int zkconnectionTimeoutMs, int zksyncTimeMs)
        {
            this.ZkConnect = zkconnect;
            this.ZkConnectionTimeoutMs = zkconnectionTimeoutMs;
            this.ZkSessionTimeoutMs = zksessionTimeoutMs;
            this.ZkSyncTimeMs = zksyncTimeMs;
        }

        public string ZkConnect { get; set; }

        public int ZkSessionTimeoutMs { get; set; }

        public int ZkConnectionTimeoutMs { get; set; }

        public int ZkSyncTimeMs { get; set; }
    }
}
