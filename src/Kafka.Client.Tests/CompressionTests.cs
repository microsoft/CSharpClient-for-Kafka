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
    using Kafka.Client.Exceptions;
    using Messages;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using TestHelper;

    [TestClass]
    public class CompressionTests
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void CompressAndDecompressMessageUsingSnappyCompressionCodec()
        {
            var messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            var message = new Message(messageBytes,CompressionCodecs.SnappyCompressionCodec);
            Message compressedMsg = CompressionUtils.Compress(new List<Message>() { message }, CompressionCodecs.SnappyCompressionCodec, 0);
            var decompressed = CompressionUtils.Decompress(compressedMsg, 0);
            int i = 0;
            foreach (var decompressedMessage in decompressed.Messages)
            {
                i++;
                message.Payload.SequenceEqual(decompressedMessage.Payload).Should().BeTrue();
            }

            Assert.AreEqual(1, i);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void CompressAndDecompress3MessagesUsingSnappyCompressionCodec()
        {
            byte[] messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            Message message1 = new Message(messageBytes);
            Message message2 = new Message(messageBytes);
            Message message3 = new Message(messageBytes);
            Message compressedMsg = CompressionUtils.Compress(new List<Message>() { message1, message2, message3 }, CompressionCodecs.SnappyCompressionCodec, 0);
            var decompressed = CompressionUtils.Decompress(compressedMsg, 0);
            int i = 0;
            foreach (var decompressedMessage in decompressed.Messages)
            {
                i++;
                message1.Payload.SequenceEqual(decompressedMessage.Payload).Should().BeTrue();
            }

            Assert.AreEqual(3, i);
        }


        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void CompressAndDecompressMessageUsingDefaultCompressionCodec()
        {
            byte[] messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            Message message = new Message(messageBytes, CompressionCodecs.DefaultCompressionCodec);
            Message compressedMsg = CompressionUtils.Compress(new List<Message>() { message }, 0);
            var decompressed = CompressionUtils.Decompress(compressedMsg, 0);
            int i = 0;
            foreach (var decompressedMessage in decompressed.Messages)
            {
                i++;
                message.Payload.SequenceEqual(decompressedMessage.Payload).Should().BeTrue();
            }

            Assert.AreEqual(1, i);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void CompressAndDecompress3MessagesUsingDefaultCompressionCodec()
        {
            byte[] messageBytes = new byte[] { 1, 2, 3, 4, 5 };
            Message message1 = new Message(messageBytes, CompressionCodecs.DefaultCompressionCodec);
            Message message2 = new Message(messageBytes, CompressionCodecs.DefaultCompressionCodec);
            Message message3 = new Message(messageBytes, CompressionCodecs.DefaultCompressionCodec);
            Message compressedMsg = CompressionUtils.Compress(new List<Message>() { message1, message2, message3 }, 0);
            var decompressed = CompressionUtils.Decompress(compressedMsg, 0);
            int i = 0;
            foreach (var decompressedMessage in decompressed.Messages)
            {
                i++;
                message1.Payload.SequenceEqual(decompressedMessage.Payload).Should().BeTrue();
            }

            Assert.AreEqual(3, i);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void CreateCompressedBufferedMessageSet()
        {
            string testMessage = "TestMessage";
            Message message = new Message(Encoding.UTF8.GetBytes(testMessage));
            BufferedMessageSet bms = new BufferedMessageSet(CompressionCodecs.DefaultCompressionCodec, new List<Message>() { message }, 0);
            foreach (var bmsMessage in bms.Messages)
            {
                Assert.AreNotEqual(bmsMessage.Payload, message.Payload);
                var decompressedBms = CompressionUtils.Decompress(bmsMessage, 0);
                foreach (var decompressedMessage in decompressedBms.Messages)
                {
                    Assert.AreEqual(message.ToString(), decompressedMessage.ToString());
                }
            }
        }

        [TestMethod]
        [ExpectedException(typeof(UnknownCodecException))]
        [TestCategory(TestCategories.BVT)]
        public void CompressionUtilsTryToCompressWithNoCompresssionCodec()
        {
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            var msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            var msg2 = new Message(payloadData2);

            CompressionUtils.Compress(new List<Message>() { msg1, msg2 }, CompressionCodecs.NoCompressionCodec, 0);
        }
    }
}
