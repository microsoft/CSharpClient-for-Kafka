// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Consumers
{
    using Kafka.Client.Responses;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Text;

    public class OffsetResponse
    {
        public int CorrelationId { get; private set; }
        public Dictionary<string, List<PartitionOffsetsResponse>> ResponseMap { get; private set; }

        public OffsetResponse(int correlationId, Dictionary<string, List<PartitionOffsetsResponse>> responseMap)
        {
            CorrelationId = correlationId;
            ResponseMap = responseMap;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(1024);
            sb.AppendFormat("OffsetResponse.CorrelationId:{0},ResponseMap Count={1}", this.CorrelationId, this.ResponseMap.Count);

            int i = 0;
            foreach (var v in this.ResponseMap)
            {
                sb.AppendFormat(",ResponseMap[{0}].Key:{1},PartitionOffsetsResponse Count={2}", i, v.Key, v.Value.Count);
                int j = 0;
                foreach (var o in v.Value)
                {
                    sb.AppendFormat(",PartitionOffsetsResponse[{0}]:{1}", j, o.ToString());
                    j++;
                }
                i++;
            }

            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }

        public class Parser : IResponseParser<OffsetResponse>
        {
            public OffsetResponse ParseFrom(KafkaBinaryReader reader)
            {
                reader.ReadInt32(); // skipping first int
                var correlationId = reader.ReadInt32();
                var numTopics = reader.ReadInt32();
                var responseMap = new Dictionary<string, List<PartitionOffsetsResponse>>();
                for (int i = 0; i < numTopics; ++i)
                {
                    var topic = reader.ReadShortString();
                    var numPartitions = reader.ReadInt32();
                    var responses = new List<PartitionOffsetsResponse>();
                    for (int p = 0; p < numPartitions; ++p)
                    {
                        responses.Add(PartitionOffsetsResponse.ReadFrom(reader));
                    }

                    responseMap[topic] = responses;
                }

                return new OffsetResponse(correlationId, responseMap);
            }
        }
    }

    public class PartitionOffsetsResponse
    {
        public int PartitionId { get; private set; }
        public ErrorMapping Error { get; private set; }
        public List<long> Offsets { get; private set; }

        public PartitionOffsetsResponse(int partitionId, ErrorMapping error, List<long> offsets)
        {
            PartitionId = partitionId;
            Error = error;
            Offsets = offsets;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(1024);

            sb.AppendFormat("PartitionOffsetsResponse.PartitionId:{0},Error:{1},Offsets Count={2}", this.PartitionId, this.Error, this.Offsets.Count);
            int i = 0;
            foreach (var o in this.Offsets)
            {
                sb.AppendFormat("Offsets[{0}]:{1}", i, o.ToString());
                i++;
            }

            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }

        public static PartitionOffsetsResponse ReadFrom(KafkaBinaryReader reader)
        {
            var partitionId = reader.ReadInt32();
            var error = reader.ReadInt16();
            var numOffsets = reader.ReadInt32();
            var offsets = new List<long>();
            for (int o = 0; o < numOffsets; ++o)
            {
                offsets.Add(reader.ReadInt64());
            }

            return new PartitionOffsetsResponse(partitionId,
                (ErrorMapping)Enum.Parse(typeof(ErrorMapping), error.ToString(CultureInfo.InvariantCulture)),
                offsets);
        }
    }
}