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


namespace Kafka.Client.Producers
{
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class TopicData
    {
        public const byte DefaultNumberOfPartitionsSize = 4;
        public const byte DefaultTopicSizeSize = 2;

        public TopicData(string topic, IEnumerable<PartitionData> partitionData)
        {
            this.Topic = topic;
            this.PartitionData = partitionData;
        }

        public string Topic { get; private set; }

        public IEnumerable<PartitionData> PartitionData { get; private set; }

        public int SizeInBytes
        {
            get
            {
                var topicLength = GetTopicLength(this.Topic);
                return DefaultTopicSizeSize + topicLength + DefaultNumberOfPartitionsSize + this.PartitionData.Sum(dataPiece => dataPiece.SizeInBytes);
            }
        }

        internal static TopicData ParseFrom(KafkaBinaryReader reader)
        {
            var topic = reader.ReadShortString();
            var partitionCount = reader.ReadInt32();
            var partitions = new PartitionData[partitionCount];
            for (int i = 0; i < partitionCount; i++)
            {
                partitions[i] = Producers.PartitionData.ParseFrom(reader);
            }
            return new TopicData(topic, partitions.OrderBy(x => x.Partition));
        }

        internal static PartitionData FindPartition(IEnumerable<PartitionData> data, int partition)
        {
            if (data == null || !data.Any())
            {
                return null;
            }

            var low = 0;
            var high = data.Count() - 1;
            while (low <= high)
            {
                var mid = (low + high) / 2;
                var found = data.ElementAt(mid);
                if (found.Partition == partition)
                {
                    return found;
                }
                else if (partition < found.Partition)
                {
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }
            return null;
        }

        protected static short GetTopicLength(string topic, string encoding = AbstractRequest.DefaultEncoding)
        {
            Encoding encoder = Encoding.GetEncoding(encoding);
            return string.IsNullOrEmpty(topic) ? AbstractRequest.DefaultTopicLengthIfNonePresent : (short)encoder.GetByteCount(topic);
        }
    }
}
