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
    using Kafka.Client.Consumers;
    using Messages;
    using Serialization;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using Utils;

    /// <summary>
    /// Constructs a request to send to Kafka.
    /// FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
    /// ReplicaId => int32
    /// MaxWaitTime => int32
    /// MinBytes => int32
    /// TopicName => string
    /// Partition => int32
    /// FetchOffset => int64
    /// MaxBytes => int32
    /// set MaxWaitTime  to 0 and MinBytes to 0 can reduce latency.
    /// </summary>
    public class FetchRequest : AbstractRequest, IWritable
    {
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultOffsetSize = 8;
        public const byte DefaultMaxSizeSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultTopicSizeSize + DefaultPartitionSize + DefaultRequestIdSize + DefaultOffsetSize + DefaultMaxSizeSize;
        public const byte DefaultHeaderAsPartOfMultirequestSize = DefaultTopicSizeSize + DefaultPartitionSize + DefaultOffsetSize + DefaultMaxSizeSize;

        public const byte DefaultVersionIdSize = 2;
        public const byte DefaultCorrelationIdSize = 4;
        public const byte DefaultReplicaIdSize = 4;
        public const byte DefaultMaxWaitSize = 4;
        public const byte DefaultMinBytesSize = 4;
        public const byte DefaultOffsetInfoSizeSize = 4;

        public const short CurrentVersion = 0;

        public FetchRequest(int correlationId, string clientId, int maxWait, int minBytes, Dictionary<string, List<PartitionFetchInfo>> fetchInfos)
        {
            this.VersionId = CurrentVersion;
            this.CorrelationId = correlationId;
            this.ClientId = clientId;
            this.ReplicaId = -1;
            this.MaxWait = maxWait;
            this.MinBytes = minBytes;
            this.OffsetInfo = fetchInfos;
            int length = GetRequestLength();
            this.RequestBuffer = new BoundedBuffer(length);
            this.WriteTo(this.RequestBuffer);
        }

        public int GetRequestLength()
        {
            return DefaultRequestSizeSize +
                   DefaultRequestIdSize +
                   DefaultVersionIdSize +
                   DefaultCorrelationIdSize +
                   BitWorks.GetShortStringLength(this.ClientId, DefaultEncoding) +
                   DefaultReplicaIdSize +
                   DefaultMaxWaitSize +
                   DefaultMinBytesSize +
                   DefaultOffsetInfoSizeSize + this.OffsetInfo.Keys.Sum(x => BitWorks.GetShortStringLength(x, DefaultEncoding)) + this.OffsetInfo.Values.Select(pl => 4 + pl.Sum(p => p.SizeInBytes)).Sum();
        }

        public short VersionId { get; private set; }
        public int CorrelationId { get; private set; }
        public string ClientId { get; private set; }
        public int ReplicaId { get; private set; }
        public int MaxWait { get; private set; }
        public int MinBytes { get; private set; }
        public Dictionary<string, List<PartitionFetchInfo>> OffsetInfo { get; set; }

        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.Fetch;
            }
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
                WriteTo(writer);
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
            writer.Write(this.ReplicaId);
            writer.Write(this.MaxWait);
            writer.Write(this.MinBytes);
            writer.Write(this.OffsetInfo.Count);
            foreach (var offsetInfo in this.OffsetInfo)
            {
                writer.WriteShortString(offsetInfo.Key);
                writer.Write(offsetInfo.Value.Count);
                foreach (var v in offsetInfo.Value)
                {
                    v.WriteTo(writer);
                }
            }
        }

        public override string ToString()
        {
            return String.Format(
                CultureInfo.CurrentCulture,
                "varsionId: {0}, correlationId: {1}, clientId: {2}, replicaId: {3}, maxWait: {4}, minBytes: {5}, requestMap: {6}",
                this.VersionId,
                this.CorrelationId,
                this.ClientId,
                this.ReplicaId,
                this.MaxWait,
                this.MinBytes,
                string.Join(";", this.OffsetInfo.Select(x => string.Format("[Topic:{0}, Info:{1}]", x.Key, string.Join("|", x.Value))))
               );
        }
    }
}
