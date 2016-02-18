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

namespace Kafka.Client.Responses
{
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class FetchResponse
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(FetchResponse));

        public FetchResponse(int correlationId, IEnumerable<TopicData> data)
        {
            Guard.NotNull(data, "data");
            this.CorrelationId = correlationId;
            this.TopicDataDict = data.GroupBy(x => x.Topic, x => x)
               .ToDictionary(x => x.Key, x => x.ToList().FirstOrDefault());
        }
        public FetchResponse(int correlationId, IEnumerable<TopicData> data, int size)
        {
            Guard.NotNull(data, "data");
            this.CorrelationId = correlationId;
            this.TopicDataDict = data.GroupBy(x => x.Topic, x => x)
               .ToDictionary(x => x.Key, x => x.ToList().FirstOrDefault());
            this.Size = size;
        }

        public int Size { get; private set; }
        public int CorrelationId { get; private set; }
        public Dictionary<string, TopicData> TopicDataDict { get; private set; }

        public BufferedMessageSet MessageSet(string topic, int partition)
        {
            var messageSet = new BufferedMessageSet(Enumerable.Empty<Message>(), partition);
            if (this.TopicDataDict.ContainsKey(topic))
            {
                var topicData = this.TopicDataDict[topic];
                if (topicData != null)
                {
                    var data = TopicData.FindPartition(topicData.PartitionData, partition);
                    if (data != null)
                    {
                        messageSet = new BufferedMessageSet(data.MessageSet.Messages, (short)data.Error, partition);
                        messageSet.HighwaterOffset = data.HighWaterMark;
                    }
                    else
                    {
                        Logger.WarnFormat("Partition data was not found for partition {0}.", partition);
                    }
                }
            }

            return messageSet;
        }

        public PartitionData PartitionData(string topic, int partition)
        {
            if (this.TopicDataDict.ContainsKey(topic))
            {
                var topicData = this.TopicDataDict[topic];
                if (topicData != null)
                {
                    return TopicData.FindPartition(topicData.PartitionData, partition);
                }
            }

            return new PartitionData(partition, new BufferedMessageSet(Enumerable.Empty<Message>(), partition));
        }

        public class Parser : IResponseParser<FetchResponse>
        {
            public FetchResponse ParseFrom(KafkaBinaryReader reader)
            {
                int size = 0, correlationId = 0, dataCount = 0;
                try
                {
                    size = reader.ReadInt32();
                    correlationId = reader.ReadInt32();
                    dataCount = reader.ReadInt32();
                    var data = new TopicData[dataCount];
                    for (int i = 0; i < dataCount; i++)
                    {
                        data[i] = TopicData.ParseFrom(reader);
                    }

                    return new FetchResponse(correlationId, data, size);
                }
                catch (OutOfMemoryException mex)
                {
                    Logger.Error(string.Format("OOM Error. Data values were: size: {0}, correlationId: {1}, dataCound: {2}.\r\nFull Stack of exception: {3}", size, correlationId, dataCount, mex.StackTrace));
                    throw;
                }
            }
        }
    }
}
