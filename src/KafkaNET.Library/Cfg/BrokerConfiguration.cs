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
    using System.Text;
    public class BrokerConfiguration
    {
        public int BrokerId { get; set; }

        public string Host { get; set; }

        public int Port { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(64);
            sb.AppendFormat("BrokerId={0},Host={1}:{2}", this.BrokerId, this.Host, this.Port);           
            return sb.ToString();
        }

        public override bool Equals(System.Object obj)
        {
            // If parameter is null return false.
            if (obj == null)
            {
                return false;
            }

            // If parameter cannot be cast to Point return false.
            BrokerConfiguration p = obj as BrokerConfiguration;
            if ((System.Object)p == null)
            {
                return false;
            }

            // Return true if the fields match:
            return (BrokerId == p.BrokerId) && (Host == p.Host) && (Port == p.Port);
        }

        public bool Equals(BrokerConfiguration p)
        {
            // If parameter is null return false:
            if ((object)p == null)
            {
                return false;
            }

            // Return true if the fields match:
            return (BrokerId == p.BrokerId) && (Host == p.Host) && (Port == p.Port);
        }

        public override int GetHashCode()
        {
            return BrokerId ^ Host.GetHashCode() ^ Port.GetHashCode();
        }

        public static string GetBrokerConfiturationString(int partitionIndex, BrokerConfiguration broker, bool isleader)
        {
            var sb = new StringBuilder();
            if (isleader)
                sb.AppendFormat("partition={0},leader[{0}]={1}", partitionIndex, broker);
            else
                sb.AppendFormat("partition={0},nonleader[{0}]={1}", partitionIndex, broker);
            return sb.ToString();
        }
    }
}
