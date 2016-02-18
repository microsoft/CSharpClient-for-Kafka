// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represent statistics or state of consumer group such as offsets positions in different topics and lags between reading position and queue size.
    /// </summary>
    public class ConsumerGroupStatisticsRecord
    {
        /// <summary>
        /// Gets or sets the name of consumer group
        /// </summary>
        public string ConsumerGroupName { get; set; }

        /// <summary>
        /// Gets or sets state for each topic that consumer groups started reading
        /// </summary>
        public IDictionary<string, TopicStatisticsRecord> TopicsStat { get; set; }

        /// <summary>
        /// Gets the total number of messages in all topics that were not consumed yet.
        /// </summary>
        public long Lag
        {
            get
            {
                if (this.TopicsStat == null)
                {
                    return 0;
                }

                long result = 0;
                foreach (var topicStatRecord in this.TopicsStat.Values)
                {
                    result += topicStatRecord.Lag;
                }

                return result;
            }
        }
    }
}
