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

using Kafka.Client.Messages;

namespace Kafka.Client.Responses
{
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System.Collections.Generic;

    public class ProducerResponseStatus
    {
        public ProducerResponseStatus()
        {
            this.Timestamp = Message.NoTimestampValue;
        }

        public ErrorMapping Error { get; set; }
        public long Offset { get; set; }
        public int ThrottleTime { get; set; }
        public long Timestamp { get; set; }
        public override string ToString()
        {
            return string.Format("Error:{0} Offset:{1}", this.Error, this.Offset);
        }
    }

    public class ProducerResponse
    {
        public ProducerResponse(int correlationId, Dictionary<TopicAndPartition, ProducerResponseStatus> statuses)
        {
            CorrelationId = correlationId;
            Statuses = statuses;
        }

        public int CorrelationId { get; set; }
        public Dictionary<TopicAndPartition, ProducerResponseStatus> Statuses { get; set; }

        public static Parser ParserForVersion(int versionId)
        {
            return new Parser(versionId);
        }

        public class Parser : IResponseParser<ProducerResponse>
        {
            private readonly int versionId;

            public Parser(int versionId)
            {
                this.versionId = versionId;
            }

            public ProducerResponse ParseFrom(KafkaBinaryReader reader)
            {
                var size = reader.ReadInt32();
                var correlationId = reader.ReadInt32();
                var topicCount = reader.ReadInt32();

                var statuses = new Dictionary<TopicAndPartition, ProducerResponseStatus>();
                for (int i = 0; i < topicCount; ++i)
                {
                    var topic = reader.ReadShortString();
                    var partitionCount = reader.ReadInt32();
                    for (int p = 0; p < partitionCount; ++p)
                    {
                        var partitionId = reader.ReadInt32();
                        var error = reader.ReadInt16();
                        var offset = reader.ReadInt64();
                        var timestamp = Message.NoTimestampValue;
                        var throttleTime = 0;

                        if (versionId >= 2)
                        {
                            timestamp = reader.ReadInt64();
                        }

                        if (versionId >= 1)
                        {
                            throttleTime = reader.ReadInt32();
                        }

                        var topicAndPartition = new TopicAndPartition(topic, partitionId);

                        statuses.Add(topicAndPartition, new ProducerResponseStatus()
                        {
                            Error = ErrorMapper.ToError(error),
                            Offset = offset,
                            ThrottleTime = throttleTime,
                            Timestamp = timestamp,
                        });

                    }
                }

                return new ProducerResponse(correlationId, statuses);
            }
        }
    }
}
