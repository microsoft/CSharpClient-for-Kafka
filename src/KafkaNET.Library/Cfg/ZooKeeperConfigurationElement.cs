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
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.Net;

    public class ZooKeeperConfigurationElement : ConfigurationElement
    {
        [ConfigurationProperty("addressList")]
        public string AddressList
        {
            get { return (string)this["addressList"]; }
            set { this["addressList"] = value; }
        }

        [ConfigurationProperty("sessionTimeout", IsRequired = false, DefaultValue = ZooKeeperConfiguration.DefaultSessionTimeout)]
        public int SessionTimeout
        {
            get { return (int)this["sessionTimeout"]; }
        }

        [ConfigurationProperty("connectionTimeout", IsRequired = false, DefaultValue = ZooKeeperConfiguration.DefaultConnectionTimeout)]
        public int ConnectionTimeout
        {
            get { return (int)this["connectionTimeout"]; }
        }

        [ConfigurationProperty("syncTime", IsRequired = false, DefaultValue = ZooKeeperConfiguration.DefaultSyncTime)]
        public int SyncTime
        {
            get { return (int)this["syncTime"]; }
        }

        [ConfigurationProperty("servers", IsRequired = false, IsDefaultCollection = false)]
        [ConfigurationCollection(typeof(ZooKeeperServerConfigurationElementCollection),
            AddItemName = "add",
            ClearItemsName = "clear",
            RemoveItemName = "remove")]
        public ZooKeeperServerConfigurationElementCollection Servers
        {
            get
            {
                return (ZooKeeperServerConfigurationElementCollection)this["servers"];
            }
        }
    }
}