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
    using System.Configuration;
    using System.Globalization;
    using System.Net;


    public class ZooKeeperServerConfigurationElement : ConfigurationElement
    {
        [ConfigurationProperty("host", IsRequired = true)]
        public string Host
        {
            get
            {
                return (string)this["host"];
            }

            set
            {
                this["host"] = value;
            }
        }

        [ConfigurationProperty("port", IsRequired = true)]
        public int Port
        {
            get
            {
                return (int)this["port"];
            }

            set
            {
                this["port"] = value;
            }
        }

        protected override void PostDeserialize()
        {
            base.PostDeserialize();
            IPAddress ipAddress;
            if (!IPAddress.TryParse(this.Host, out ipAddress))
            {
                var addresses = Dns.GetHostAddresses(this.Host);
                if (addresses.Length > 0)
                {
                    this.Host = addresses[0].ToString();
                }
                else
                {
                    throw new ConfigurationErrorsException(string.Format(CultureInfo.CurrentCulture, "Could not resolve the address: {0}.", this.Host));
                }
            }
        }
    }
}
