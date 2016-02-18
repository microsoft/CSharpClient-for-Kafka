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
    using Kafka.Client.Messages;
    using Kafka.Client.Responses;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// Constructs a request to send to Kafka to get the current offset for a given topic
    /// </summary>
    public class OffsetRequest : AbstractRequest, IWritable
    {
        /// <summary>
        /// The latest time constant.
        /// </summary>
        public static readonly long LatestTime = -1L;

        /// <summary>
        /// The earliest time constant.
        /// </summary>
        public static readonly long EarliestTime = -2L;

        public const string SmallestTime = "smallest";
        public const string LargestTime = "largest";
        public const byte DefaultTopicSizeSize = 2;
        public const byte DefaultPartitionSize = 4;
        public const byte DefaultTimeSize = 8;
        public const byte DefaultReplicaIdSize = 4;
        public const byte DefaultRequestInfoSize = 4;
        public const byte DefaultPartitionCountSize = 4;
        public const byte DefaultHeaderSize = DefaultRequestSizeSize + DefaultRequestIdSize +
                                              2 + // version
                                              4;  // correlation            

        public int GetRequestLength()
        {
            return DefaultHeaderSize +
                   GetShortStringWriteLength(ClientId) +
                   DefaultReplicaIdSize +
                   DefaultRequestInfoSize +
                   RequestInfo.Keys.Sum(k => GetShortStringWriteLength(k)) +
                   RequestInfo.Values.Sum(v => DefaultPartitionCountSize + v.Count * PartitionOffsetRequestInfo.SizeInBytes);
        }

        /// <summary>
        /// Initializes a new instance of the OffsetRequest class.
        /// </summary>        
        public OffsetRequest(Dictionary<string,
            List<PartitionOffsetRequestInfo>> requestInfo,
            short versionId = 0,
            int correlationId = 0,
            string clientId = "",
            int replicaId = -1)
        {
            VersionId = versionId;
            ClientId = clientId;
            CorrelationId = correlationId;
            ReplicaId = replicaId;
            RequestInfo = requestInfo;
            this.RequestBuffer = new BoundedBuffer(GetRequestLength());
            this.WriteTo(this.RequestBuffer);
        }

        public short VersionId { get; private set; }
        public int ReplicaId { get; private set; }
        public int CorrelationId { get; private set; }
        public string ClientId { get; private set; }
        public Dictionary<string, List<PartitionOffsetRequestInfo>> RequestInfo { get; private set; }
        public override RequestTypes RequestType
        {
            get
            {
                return RequestTypes.Offsets;
            }
        }

        /// <summary>
        /// Writes content into given stream
        /// </summary>
        /// <param name="output">
        /// The output stream.
        /// </param>
        public void WriteTo(System.IO.MemoryStream output)
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

            writer.Write(VersionId);
            writer.Write(CorrelationId);
            writer.WriteShortString(ClientId);
            writer.Write(ReplicaId);
            writer.Write(RequestInfo.Count);
            foreach (var kv in RequestInfo)
            {
                writer.WriteShortString(kv.Key);
                writer.Write(kv.Value.Count);
                foreach (var info in kv.Value)
                {
                    info.WriteTo(writer);
                }
            }
        }
    }

    public class PartitionOffsetRequestInfo : IWritable
    {
        public int PartitionId { get; private set; }
        public long Time { get; private set; }
        public int MaxNumOffsets { get; private set; }

        public PartitionOffsetRequestInfo(int partitionId, long time, int maxNumOffsets)
        {
            PartitionId = partitionId;
            Time = time;
            MaxNumOffsets = maxNumOffsets;
        }

        public static int SizeInBytes
        {
            get { return 4 + 8 + 4; }
        }

        public void WriteTo(MemoryStream output)
        {
            using (var writer = new KafkaBinaryWriter(output))
            {
                WriteTo(writer);
            }
        }

        public void WriteTo(KafkaBinaryWriter writer)
        {
            writer.Write(PartitionId);
            writer.Write(Time);
            writer.Write(MaxNumOffsets);
        }
    }
}
