// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Consumers
{
    /// <summary>
    /// Represent the statistics or state of consuming a partition
    /// </summary>
    public class PartitionStatisticsRecord
    {
        /// <summary>
        /// Gets or sets the Partition Id
        /// </summary>
        public int PartitionId { get; set; }

        /// <summary>
        /// Gets or sets the ConsumerId that currently owns reading the partition.
        /// </summary>
        public string OwnerConsumerId { get; set; }

        /// <summary>
        /// Gets or sets offset of last committed message read from the partition
        /// </summary>
        public long CurrentOffset { get; set; }

        /// <summary>
        /// Gets or sets offset of last message written to the partition
        /// </summary>
        public long LastOffset { get; set; }

        /// <summary>
        /// Gets the number of messages between last consumed message and last message in partition
        /// </summary>
        public long Lag
        {
            get
            {
                return this.LastOffset - this.CurrentOffset;
            }
        }
    }
}
