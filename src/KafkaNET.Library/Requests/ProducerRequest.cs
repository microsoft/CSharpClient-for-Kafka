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
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Linq;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Kafka.Client.Producers;
    using Kafka.Client.Responses;

    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class ProducerRequest : AbstractRequest, IWritable
    {
        public const int RandomPartition = -1;
        public const short CurrentVersion = 0;
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultSetSizeSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize +
            DefaultTopicSizeSize +
            DefaultPartitionSize +
            DefaultRequestIdSize +
            DefaultSetSizeSize;

        public const byte VersionIdSize = 2;
        public const byte CorrelationIdSize = 4;
        public const byte ClientIdSize = 2;
        public const byte RequiredAcksSize = 2;
        public const byte AckTimeoutSize = 4;
        public const byte NumberOfTopicsSize = 4;
        public const byte DefaultHeaderSize8 = DefaultRequestSizeSize +
            DefaultRequestIdSize +
            VersionIdSize +
            CorrelationIdSize +
            RequiredAcksSize +
            AckTimeoutSize +
            NumberOfTopicsSize;

        public ProducerRequest(short versionId, int correlationId, string clientId, short requiredAcks, int ackTimeout, IEnumerable<TopicData> data)
        {
            this.VersionId = versionId;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.RequiredAcks = requiredAcks;
            this.AckTimeout = ackTimeout;
            this.Data = data;
            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }
        /// <summary>
        /// This function should be obsolete since it's not sync with java version.
        /// </summary>
        /// <param name="correlationId"></param>
        /// <param name="clientId"></param>
        /// <param name="requiredAcks"></param>
        /// <param name="ackTimeout"></param>
        /// <param name="data"></param>
        public ProducerRequest(int correlationId, string clientId, short requiredAcks, int ackTimeout, IEnumerable<TopicData> data)
            : this(CurrentVersion, correlationId, clientId, requiredAcks, ackTimeout, data)
        {
        }
        /// <summary>
        /// Based on messageset per topic/partition, group it by topic and send out.
        /// Sync with java/scala version , do group by topic inside this class.
        /// https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/api/ProducerRequest.scala;h=570b2da1d865086f9830aa919a49063abbbe574d;hb=HEAD
        /// private lazy val dataGroupedByTopic = data.groupBy(_._1.topic)
        /// </summary>
        /// <param name="correlationId"></param>
        /// <param name="clientId"></param>
        /// <param name="requiredAcks"></param>
        /// <param name="ackTimeout"></param>
        /// <param name="messagesPerTopic"></param>
        public ProducerRequest(int correlationId, string clientId, short requiredAcks, int ackTimeout, IDictionary<TopicAndPartition, BufferedMessageSet> messagesPerTopic)
        {
            string topicName = string.Empty;
            int partitionId = -1;
            var topics = new Dictionary<string, List<PartitionData>>();
            foreach (KeyValuePair<TopicAndPartition, BufferedMessageSet> keyValuePair in messagesPerTopic)
            {
                topicName = keyValuePair.Key.Topic;
                partitionId = keyValuePair.Key.PartitionId;
                var messagesSet = keyValuePair.Value;
                if (!topics.ContainsKey(topicName))
                {
                    topics.Add(topicName, new List<PartitionData>()); //create a new list for this topic
                }
                topics[topicName].Add(new PartitionData(partitionId, messagesSet));
            }

            this.VersionId = CurrentVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.RequiredAcks = requiredAcks;
            this.AckTimeout = ackTimeout;
            this.Data = topics.Select(kv => new TopicData(kv.Key, kv.Value));
            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        public short VersionId { get; set; }
        public int CorrelationId { get; set; }
        public string ClientId { get; set; }
        public short RequiredAcks { get; set; }
        public int AckTimeout { get; set; }
        public IEnumerable<TopicData> Data { get; set; }
        public BufferedMessageSet MessageSet { get; private set; }
        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.Produce;
            }
        }
        public int TotalSize
        {
            get
            {
                return (int)this.RequestBuffer.Length;
            }
        }

        public int GetRequestLength()
        {
            return DefaultHeaderSize8 +
                GetShortStringWriteLength(this.ClientId) +
                this.Data.Sum(item => item.SizeInBytes);
        }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
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

        /// <summary>
        /// Writes content into given writer
        /// </summary>
        /// <param name="writer">
        /// The writer.
        /// </param>
        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");

            writer.Write(this.VersionId);
            writer.Write(this.CorrelationId);
            writer.WriteShortString(this.ClientId);
            writer.Write(this.RequiredAcks);
            writer.Write(this.AckTimeout);
            writer.Write(this.Data.Count());
            foreach (var topicData in this.Data)
            {
                writer.WriteShortString(topicData.Topic);
                writer.Write(topicData.PartitionData.Count());
                foreach (var partitionData in topicData.PartitionData)
                {
                    writer.Write(partitionData.Partition);
                    writer.Write(partitionData.MessageSet.SetSize);
                    partitionData.MessageSet.WriteTo(writer);
                }
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Request size: ");
            sb.Append(this.TotalSize);
            sb.Append(", RequestId: ");
            sb.Append(this.RequestTypeId);
            sb.Append("(");
            sb.Append((RequestTypes)this.RequestTypeId);
            sb.Append(")");
            sb.Append(", ClientId: ");
            sb.Append(this.ClientId);
            sb.Append(", Version: ");
            sb.Append(this.VersionId);
            sb.Append(", Set size: ");
            sb.Append(this.MessageSet.SetSize);
            sb.Append(", Set {");
            sb.Append(this.MessageSet.ToString());
            sb.Append("}");
            return sb.ToString();
        }
    }
}
