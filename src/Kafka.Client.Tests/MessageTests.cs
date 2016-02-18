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
    using Messages;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Serialization;
    using System.IO;
    using System.Linq;
    using System.Text;
    using TestHelper;
    using Utils;

    /// <summary>
    /// Tests for the <see cref="Message"/> class.
    /// </summary>
    [TestClass]
    public class MessageTests
    {
        /// <summary>
        /// Ensure that the bytes returned from the message are in valid kafka sequence.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void GetBytesValidSequence()
        {
            var payload = Encoding.UTF8.GetBytes("kafka");
            Message message = new Message(payload, CompressionCodecs.NoCompressionCodec);

            MemoryStream ms = new MemoryStream();
            message.WriteTo(ms);

            Assert.AreEqual(message.Size, ms.Length);

            var crc = Crc32Hasher.ComputeCrcUint32(ms.GetBuffer(), 4, (int)(ms.Length - 4));

            // first 4 bytes = the crc
            using (var reader = new KafkaBinaryReader(ms))
            {
                Assert.AreEqual(crc, reader.ReadUInt32());

                // magic
                Assert.AreEqual(message.Magic, reader.ReadByte());

                // attributes
                Assert.AreEqual((byte)0, reader.ReadByte());

                // key size
                Assert.AreEqual(-1, reader.ReadInt32());

                // payload size
                Assert.AreEqual(payload.Length, reader.ReadInt32());

                // remaining bytes = the payload
                payload.SequenceEqual(reader.ReadBytes(10)).Should().BeTrue();
            }
        }

    }
}
