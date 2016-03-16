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

namespace Kafka.Client.Tests
{
    using FluentAssertions;
    using Kafka.Client.Serialization;
    using Messages;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using TestHelper;
    using Utils;

    [TestClass]
    public class MessageSetTests
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void BufferedMessageSetWriteToValidSequenceForV0Message()
        {
            RunMessageSetWriteValidSequestTest(false);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void BufferedMessageSetWriteToValidSequenceForV1Message()
        {
            RunMessageSetWriteValidSequestTest(true);
        }

        private void RunMessageSetWriteValidSequestTest(bool useV1Message)
        {
            byte[] messageBytes = { 1, 2, 3, 4, 5 };
            Message msg1, msg2;

            if (useV1Message)
            {
                msg1 = new Message(123L, TimestampTypes.CreateTime, messageBytes) {Offset = 0};
                msg2 = new Message(123L, TimestampTypes.CreateTime, messageBytes);
            }
            else
            {
                msg1 = new Message(messageBytes) {Offset = 0};
                msg2 = new Message(messageBytes);
            }

            msg2.Offset = 1;
            MessageSet messageSet = new BufferedMessageSet(new List<Message>() { msg1, msg2 }, 0);
            var ms = new MemoryStream();
            messageSet.WriteTo(ms);

            var reader = new KafkaBinaryReader(ms);
            int baseOffset = 0;
            for (int i = 0; i < 2; ++i)
            {
                reader.ReadInt64().Should().Be(i); // offset
                var msgLength = reader.ReadInt32(); // length
                msgLength.Should().Be((useV1Message ? Message.V1HeaderSize : Message.V0HeaderSize) + msg1.PayloadSize);
                reader.ReadUInt32().Should().Be(Crc32Hasher.ComputeCrcUint32(ms.GetBuffer(), baseOffset + 8 + 4 + 4, msgLength - 4));
                reader.ReadByte().Should().Be(useV1Message ? (byte)1 : (byte)0); // magic
                reader.ReadByte().Should().Be(msg1.Attributes);
                if (useV1Message) reader.ReadInt64().Should().Be(123L); // Timestamp
                reader.ReadInt32().Should().Be(-1); // key length
                reader.ReadInt32().Should().Be(messageBytes.Length); // message length
                reader.ReadBytes(messageBytes.Length).SequenceEqual(messageBytes).Should().BeTrue();
                baseOffset += 8 + 4 + msgLength;
            }
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void SetSizeValid()
        {
            for (var i = 0; i < 2; i++)
            {
                var useV0Message = i == 1;
                byte[] messageBytes = new byte[] {1, 2, 3, 4, 5};
                Message msg1, msg2;

                if (useV0Message)
                {
                    msg1 = new Message(messageBytes);
                    msg2 = new Message(messageBytes);
                }
                else
                {
                    msg1 = new Message(123L, TimestampTypes.CreateTime, messageBytes);
                    msg2 = new Message(123L, TimestampTypes.CreateTime, messageBytes);
                }

                MessageSet messageSet = new BufferedMessageSet(new List<Message>() {msg1, msg2}, 0);
                Assert.AreEqual(
                    2 * (8 + 4 + (useV0Message ? Message.V0HeaderSize : Message.V1HeaderSize) + messageBytes.Length),
                    messageSet.SetSize);
            }
        }
    }
}
