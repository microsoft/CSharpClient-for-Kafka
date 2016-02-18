// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Cluster
{
    using System.Runtime.Serialization;

    [DataContract]
    public class ZkBrokerInfo
    {
        [DataMember(Name = "host")] public string Host;
        [DataMember(Name = "jmx_port")] public int JmxPort;
        [DataMember(Name = "timestamp")] public long Timestamp;
        [DataMember(Name = "version")] public int version;
    }
}
