// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Producers.Partitioning
{
    public class ModPartitionerForObjectKeyType : IPartitioner<object>
    {
        public int Partition(object key, int numPartitions)
        {
            return key.GetHashCode() % numPartitions;
        }
    }
}
