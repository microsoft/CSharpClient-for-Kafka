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
    using Kafka.Client.Serialization;
    using Messages;
    using System.Collections.Generic;
    using Utils;

    /// <summary>
    /// PartitionData, contains buffered messageset
    /// </summary>
    public class PartitionData
    {
        public const byte DefaultPartitionIdSize = 4;
        public const byte DefaultMessagesSizeSize = 4;

        public PartitionData(int partition, ErrorMapping error, BufferedMessageSet messages)
        {
            this.Partition = partition;
            this.MessageSet = messages;
            this.Error = error;
            this.HighWaterMark = messages.HighwaterOffset;
        }

        public PartitionData(int partition, BufferedMessageSet messages)
            : this(partition, (short) ErrorMapping.NoError, messages)
        {
        }

        public int Partition { get; private set; }
        public BufferedMessageSet MessageSet { get; private set; }
        public long HighWaterMark { get; private set; }
        public ErrorMapping Error { get; private set; }
        public int SizeInBytes
        {
            get { return DefaultPartitionIdSize + DefaultMessagesSizeSize + this.MessageSet.SetSize; }
        }

        internal static PartitionData ParseFrom(KafkaBinaryReader reader)
        {
            var partition = reader.ReadInt32();
            var error = reader.ReadInt16();
            var highWatermark = reader.ReadInt64();
            var messageSetSize = reader.ReadInt32();
            var bufferedMessageSet = BufferedMessageSet.ParseFrom(reader, messageSetSize, partition);
            return new PartitionData(partition, ErrorMapper.ToError(error), bufferedMessageSet);
        }

        public List<MessageAndOffset> GetMessageAndOffsets()
        {
            List<MessageAndOffset> listMessageAndOffsets = new List<MessageAndOffset>();
            //Seemly the MessageSet can only do traverse for one time.
            foreach (MessageAndOffset m in this.MessageSet)
            {
                listMessageAndOffsets.Add(m);
            }
            return listMessageAndOffsets;
        }
    }
}
