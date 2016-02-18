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
        public void BufferedMessageSetWriteToValidSequence()
        {
            byte[] messageBytes = { 1, 2, 3, 4, 5 };
            var msg1 = new Message(messageBytes) { Offset = 0 };
            var msg2 = new Message(messageBytes);
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
                msgLength.Should().Be(Message.DefaultHeaderSize + msg1.PayloadSize);
                reader.ReadUInt32().Should().Be(Crc32Hasher.ComputeCrcUint32(ms.GetBuffer(), baseOffset + 8 + 4 + 4, msgLength - 4));
                reader.ReadByte().Should().Be(0); // magic                
                reader.ReadByte().Should().Be(msg1.Attributes);
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
            byte[] messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            Message msg1 = new Message(messageBytes);
            Message msg2 = new Message(messageBytes);
            MessageSet messageSet = new BufferedMessageSet(new List<Message>() { msg1, msg2 }, 0);
            Assert.AreEqual(
                2 * (8 + 4 + Message.DefaultHeaderSize + messageBytes.Length),
                messageSet.SetSize);
        }
    }
}
