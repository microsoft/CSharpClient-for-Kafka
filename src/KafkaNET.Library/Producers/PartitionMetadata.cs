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
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// TODO: Update summary.
    /// </summary>
    public class PartitionMetadata : IWritable
    {
        public const int DefaultPartitionIdSize = 4;
        public const int DefaultIfLeaderExistsSize = 1;
        public const int DefaultNumberOfReplicasSize = 2;
        public const int DefaultNumberOfSyncReplicasSize = 2;
        public const int DefaultIfLogSegmentMetadataExistsSize = 1;

        public PartitionMetadata(int partitionId, Broker leader, IEnumerable<Broker> replicas, IEnumerable<Broker> isr)
        {
            this.PartitionId = partitionId;
            this.Leader = leader;
            this.Replicas = replicas;
            this.Isr = isr;
        }

        public int PartitionId { get; private set; }
        public Broker Leader { get; private set; }
        public IEnumerable<Broker> Replicas { get; private set; }
        public IEnumerable<Broker> Isr { get; private set; }
        public int SizeInBytes
        {
            get
            {
                var size = DefaultPartitionIdSize;
                if (this.Leader != null)
                {
                    size += this.Leader.SizeInBytes;
                }
                size += DefaultNumberOfReplicasSize;
                size += Replicas.Sum(replica => replica.SizeInBytes);
                size += DefaultNumberOfSyncReplicasSize;
                size += Isr.Sum(isr => isr.SizeInBytes);
                size += DefaultIfLogSegmentMetadataExistsSize;
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

            // if leader exists
            writer.Write(this.PartitionId);
            if (this.Leader != null)
            {
                writer.Write((byte)1);
                this.Leader.WriteTo(writer);
            }
            else
            {
                writer.Write((byte)0);
            }

            // number of replicas
            writer.Write((short)Replicas.Count());
            foreach (var replica in Replicas)
            {
                replica.WriteTo(writer);
            }

            // number of in-sync replicas
            writer.Write((short)this.Isr.Count());
            foreach (var isr in Isr)
            {
                isr.WriteTo(writer);
            }

            writer.Write((byte)0);
        }

        public static PartitionMetadata ParseFrom(KafkaBinaryReader reader, Dictionary<int, Broker> brokers)
        {
            var errorCode = reader.ReadInt16();
            var partitionId = reader.ReadInt32();
            var leaderId = reader.ReadInt32();
            Broker leader = null;
            if (leaderId != -1)
            {
                leader = brokers[leaderId];
            }
            
            // list of all replicas
            var numReplicas = reader.ReadInt32();
            var replicas = new List<Broker>();
            for (int i = 0; i < numReplicas; ++i)
            {
                replicas.Add(brokers[reader.ReadInt32()]);
            }

            // list of in-sync replicas
            var numIsr = reader.ReadInt32();
            var isrs = new List<Broker>();
            for (int i = 0; i < numIsr; ++i)
            {
                isrs.Add(brokers[reader.ReadInt32()]);
            }
                        
            return new PartitionMetadata(partitionId, leader, replicas, isrs);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(4096);
            sb.AppendFormat(
                "PartitionMetadata.ParitionId:{0},Leader:{1},Replicas Count:{2},Isr Count:{3}",
                this.PartitionId,
                this.Leader == null ? "null" : this.Leader.ToString(),
                this.Replicas.Count(),
                this.Isr.Count());

            int i = 0;
            foreach (var r in this.Replicas)
            {
                sb.AppendFormat(",Replicas[{0}]:{1}", i, r.ToString());
                i++;
            }

            i = 0;
            foreach (var sr in this.Isr)
            {
                sb.AppendFormat(",Isr[{0}]:{1}", i, sr.ToString());
                i++;
            }

            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }
    }
}
