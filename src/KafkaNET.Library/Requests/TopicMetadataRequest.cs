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

namespace Kafka.Client.Requests
{
    using Kafka.Client.Cluster;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Responses;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    /// <summary>
    /// Kafka request to get topic metadata.
    /// </summary>
    public class TopicMetadataRequest : AbstractRequest, IWritable
    {
        private const int DefaultNumberOfTopicsSize = 4;
        private const byte DefaultHeaderSize8 = DefaultRequestSizeSize + DefaultRequestIdSize;

        private readonly short versionId;
        private readonly int correlationId;
        private readonly string clientId;

        private TopicMetadataRequest(IEnumerable<string> topics, short versionId, int correlationId, string clientId)
        {
            if (topics == null)
            {
                throw new ArgumentNullException("topics", "List of topics cannot be null.");
            }

            if (!topics.Any())
            {
                throw new ArgumentException("List of topics cannot be empty.");
            }

            this.Topics = new List<string>(topics);
            this.versionId = versionId;
            this.correlationId = correlationId;
            this.clientId = clientId;
            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        public IEnumerable<string> Topics { get; private set; }
        public DetailedMetadataRequest DetailedMetadata { get; private set; }

        /// <summary>
        /// Creates simple request with no segment metadata information
        /// </summary>
        /// <param name="topics">list of topics</param>
        /// <param name="versionId"></param>
        /// <param name="correlationId"></param>
        /// <param name="clientId"></param>
        /// <returns>request</returns>
        public static TopicMetadataRequest Create(IEnumerable<string> topics, short versionId, int correlationId, string clientId)
        {
            return new TopicMetadataRequest(topics, versionId, correlationId, clientId);
        }

        public override RequestTypes RequestType
        {
            get { return RequestTypes.TopicMetadataRequest; }
        }

        public void WriteTo(MemoryStream output)
        {
            Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                writer.Write(this.RequestBuffer.Capacity - DefaultRequestSizeSize);
                writer.Write(this.RequestTypeId);
                this.WriteTo(writer);
            }
        }

        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");
            writer.Write(versionId);
            writer.Write(correlationId);
            writer.WriteShortString(clientId, DefaultEncoding);
            writer.Write(this.Topics.Count());
            foreach (var topic in Topics)
            {
                writer.WriteShortString(topic, DefaultEncoding);
            }
        }

        public int GetRequestLength()
        {
            var size = DefaultHeaderSize8 +
                FetchRequest.DefaultVersionIdSize +
                FetchRequest.DefaultCorrelationIdSize +
                BitWorks.GetShortStringLength(this.clientId, DefaultEncoding) +
                DefaultNumberOfTopicsSize +
                this.Topics.Sum(x => BitWorks.GetShortStringLength(x, DefaultEncoding));

            return size;
        }
        /// <summary>
        /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
        /// MetadataResponse => [Broker][TopicMetadata]
        ///   Broker => NodeId Host Port
        ///     NodeId => int32
        ///     Host => string
        ///     Port => int32
        ///   TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
        ///     TopicErrorCode => int16
        ///     PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
        ///         PartitionErrorCode => int16
        ///         PartitionId => int32
        ///         Leader => int32
        ///         Replicas => [int32]
        ///         Isr => [int32]
        /// </summary>
        public class Parser : IResponseParser<IEnumerable<TopicMetadata>>
        {
            public IEnumerable<TopicMetadata> ParseFrom(KafkaBinaryReader reader)
            {
                reader.ReadInt32();
                int correlationId = reader.ReadInt32();
                int brokerCount = reader.ReadInt32();
                var brokerMap = new Dictionary<int, Broker>();
                for (int i = 0; i < brokerCount; ++i)
                {
                    var broker = Broker.ParseFrom(reader);
                    brokerMap[broker.Id] = broker;
                }

                var numTopics = reader.ReadInt32();
                var topicMetadata = new TopicMetadata[numTopics];
                for (int i = 0; i < numTopics; i++)
                {
                    topicMetadata[i] = TopicMetadata.ParseFrom(reader, brokerMap);
                }
                return topicMetadata;
            }
        }
    }

    public enum DetailedMetadataRequest : short
    {
        SegmentMetadata = (short)1,
        NoSegmentMetadata = (short)0,
    }
}
