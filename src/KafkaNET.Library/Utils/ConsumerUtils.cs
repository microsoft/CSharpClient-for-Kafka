// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Utils
{
    using Kafka.Client.Consumers;
    using Kafka.Client.Requests;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// helper methods to use with Kafka <see cref="Kafka.Client.Consumers.Consumer"/> instances
    /// </summary>
    public static class ConsumerUtils
    {
        /// <summary>
        /// Retrive first or last offset for a given partition.
        /// </summary>
        /// <remarks>
        /// If <see cref="offsetRequestConstant"/> not equal to <see cref="OffsetRequest.LatestTime"/> or <see cref="OffsetRequest.EarliestTime"/> method returns offset before specified number.
        /// </remarks>
        /// <param name="consumer">Consumer instance</param>
        /// <param name="topic">The topic</param>
        /// <param name="partitionId">Partition Id</param>
        /// <param name="offsetRequestConstant">Offset that indicates what offset need to return.</param>
        /// <returns>
        /// Retrive first or last offset for a given partition based on <see cref="offsetRequestConstant"/> parameter.
        /// If offset couldn't be retrieved returns null.
        /// </returns>
        public static long? EarliestOrLatestOffset(Consumer consumer, string topic, int partitionId, long offsetRequestConstant)
        {
            var requestInfos = new Dictionary<string, List<PartitionOffsetRequestInfo>>();
            requestInfos[topic] = new List<PartitionOffsetRequestInfo>() { new PartitionOffsetRequestInfo(partitionId, offsetRequestConstant, 1) };
            var offsets = consumer.GetOffsetsBefore(new OffsetRequest(requestInfos));
            var topicResult = offsets.ResponseMap[topic];
            var responseRow = topicResult != null ? topicResult.FirstOrDefault() : null;
            return responseRow != null && responseRow.Offsets != null && responseRow.Offsets.Count > 0 ? responseRow.Offsets[0] : (long?)null;
        }
    }
}
