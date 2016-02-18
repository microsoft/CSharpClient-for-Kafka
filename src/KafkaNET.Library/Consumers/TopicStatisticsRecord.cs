// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represent current state of consuming specified topic
    /// </summary>
    public class TopicStatisticsRecord
    {
        /// <summary>
        /// Gets or sets topic name
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// Gets or sets statistics for each partition within a topic
        /// </summary>
        public IDictionary<int, PartitionStatisticsRecord> PartitionsStat { get; set; }

        /// <summary>
        /// Gets the total number of messages in all topics that were not consumed yet.
        /// </summary>
        public long Lag
        {
            get
            {
                if (this.PartitionsStat == null)
                {
                    return 0;
                }

                long result = 0;
                foreach (var partitionStatRecord in this.PartitionsStat.Values)
                {
                    result += partitionStatRecord.Lag;
                }

                return result;
            }
        }
    }
}
