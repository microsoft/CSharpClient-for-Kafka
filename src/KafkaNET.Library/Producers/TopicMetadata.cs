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
    using Kafka.Client.Cluster;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using Serialization;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class TopicMetadata : IWritable
    {
        public const byte DefaultNumOfPartitionsSize = 4;

        public TopicMetadata(string topic, IEnumerable<PartitionMetadata> partitionsMetadata, ErrorMapping error)
        {
            this.Topic = topic;
            this.PartitionsMetadata = partitionsMetadata;
            Error = error;
        }


        public string Topic { get; private set; }
        public IEnumerable<PartitionMetadata> PartitionsMetadata { get; private set; }
        public ErrorMapping Error { get; private set; }
        public int SizeInBytes
        {
            get
            {
                var size = (int)BitWorks.GetShortStringLength(this.Topic, AbstractRequest.DefaultEncoding);
                foreach (var partitionMetadata in this.PartitionsMetadata)
                {
                    size += DefaultNumOfPartitionsSize + partitionMetadata.SizeInBytes;
                }
                return size;
            }
        }

        public void WriteTo(System.IO.MemoryStream output)
        {
            Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                this.WriteTo(writer);
            }
        }

        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");

            writer.WriteShortString(this.Topic, AbstractRequest.DefaultEncoding);
            writer.Write(this.PartitionsMetadata.Count());
            foreach (var partitionMetadata in PartitionsMetadata)
            {
                partitionMetadata.WriteTo(writer);
            }
        }

        internal static TopicMetadata ParseFrom(KafkaBinaryReader reader, Dictionary<int, Broker> brokers)
        {
            var errorCode = reader.ReadInt16();
            var topic = BitWorks.ReadShortString(reader, AbstractRequest.DefaultEncoding);
            var numPartitions = reader.ReadInt32();
            var partitionsMetadata = new List<PartitionMetadata>();
            for (int i = 0; i < numPartitions; i++)
            {
                partitionsMetadata.Add(PartitionMetadata.ParseFrom(reader, brokers));
            }
            return new TopicMetadata(topic, partitionsMetadata, ErrorMapper.ToError(errorCode));
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(1024);
            sb.AppendFormat("TopicMetaData.Topic:{0},Error:{1},PartitionMetaData Count={2}", this.Topic, this.Error.ToString(), this.PartitionsMetadata.Count());
            sb.AppendLine();

            int j = 0;
            foreach (var p in this.PartitionsMetadata)
            {
                sb.AppendFormat("PartitionMetaData[{0}]:{1}", j, p.ToString());
                j++;
            }

            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }
    }
}
