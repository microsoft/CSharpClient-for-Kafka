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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestHelper;

namespace Kafka.Client.Tests.Request
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Messages;
    using Client.Producers;
    using Requests;
    using Utils;

    [TestClass]
    public class ProducerRequestTests
    {
        /// <summary>
        /// Tests to ensure that the request follows the expected structure.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void GetBytesValidStructure()
        {
            string topicName = "topic";
            int correlationId = 1;
            string clientId = "TestClient";
            short requiredAcks = 5;
            int ackTimeout = 345;

            var partition = 2;
            short error = 0;
            var payload = Encoding.UTF8.GetBytes("testMessage");
            BufferedMessageSet messageSet = new BufferedMessageSet(new List<Message>() { new Message(payload) }, 0);

            var partitionData = new PartitionData(partition, ErrorMapper.ToError(error), messageSet);

            var topicData = new TopicData(topicName, new List<PartitionData>() { partitionData });

            var request = new ProducerRequest(correlationId, clientId, requiredAcks, ackTimeout, new List<TopicData>() { topicData });

            int requestSize = 2 + //request type id
                              2 + //versionId
                              4 + //correlation id
                              request.GetShortStringWriteLength(clientId) + // actual client id
                              2 + //required acks
                              4 + //ack timeout
                              4 + //data count
                                  //=== data part
                              request.GetShortStringWriteLength(topicName) + //topic
                              4 + //partition data count
                              4 + //partition id                              
                              4 + //messages set size
                              messageSet.SetSize;

            var ms = new MemoryStream();
            request.WriteTo(ms);
            byte[] bytes = ms.ToArray();
            Assert.IsNotNull(bytes);

            // add 4 bytes for the length of the message at the beginning
            Assert.AreEqual(requestSize + 4, bytes.Length);

            // first 4 bytes = the message length
            Assert.AreEqual(requestSize, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the request type
            Assert.AreEqual((short)RequestTypes.Produce, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the version id
            Assert.AreEqual((short)ProducerRequest.CurrentVersion, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the correlation id
            Assert.AreEqual(correlationId, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(8).Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the client id length
            Assert.AreEqual((short)clientId.Length, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(12).Take(2).ToArray<byte>()), 0));

            // next few bytes = the client id
            Assert.AreEqual(clientId, Encoding.ASCII.GetString(bytes.Skip(14).Take(clientId.Length).ToArray<byte>()));

            // next 2 bytes = the required acks
            Assert.AreEqual((short)requiredAcks, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(14 + clientId.Length).Take(2).ToArray<byte>()), 0));

            // next 4 bytes = the ack timeout
            Assert.AreEqual(ackTimeout, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(16 + clientId.Length).Take(4).ToArray<byte>()), 0));

            // next 4 bytes = the data count
            Assert.AreEqual(1, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(20 + clientId.Length).Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the tppic length
            Assert.AreEqual((short)topicName.Length, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(24 + clientId.Length).Take(2).ToArray<byte>()), 0));

            // next few bytes = the topic
            Assert.AreEqual(topicName, Encoding.ASCII.GetString(bytes.Skip(26 + clientId.Length).Take(topicName.Length).ToArray<byte>()));

            // next 4 bytes = the partition data count
            Assert.AreEqual(topicData.PartitionData.Count(), BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(26 + clientId.Length + topicName.Length).Take(4).ToArray<byte>()), 0));

            // next 4 bytes = the partition
            Assert.AreEqual(partition, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(30 + clientId.Length + topicName.Length).Take(4).ToArray<byte>()), 0));

            // skipping MessageSet check - this could be done separately in another unit test
        }
    }
}
