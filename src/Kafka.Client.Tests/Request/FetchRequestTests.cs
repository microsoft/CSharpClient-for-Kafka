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



namespace Kafka.Client.Tests.Request
{
    using Consumers;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Requests;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using TestHelper;
    using Utils;

    /// <summary>
    /// Tests for the <see cref="FetchRequest"/> class.
    /// </summary>
    [TestClass]
    public class FetchRequestTests
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
            int maxWait = 234;
            int minBytes = 345;

            var requestMap = new Dictionary<string, List<PartitionFetchInfo>>();

            requestMap[topicName] = new List<PartitionFetchInfo>() {new PartitionFetchInfo(2, 4000, 777)};


            var request = new FetchRequest(correlationId, clientId, maxWait, minBytes, requestMap);


            int requestSize = 4 + //request size
                              2 + //request type id
                              2 + //versionId
                              4 + //correlation id
                              BitWorks.GetShortStringLength(clientId, AbstractRequest.DefaultEncoding) + // client id length
                              4 + //replica id
                              4 + //max wait
                              4 + //min bytes
                              4 + //offset info count
                              //=== offset info part
                              request.OffsetInfo.Keys.Sum(x => BitWorks.GetShortStringLength(x, AbstractRequest.DefaultEncoding)) +
                              request.OffsetInfo.Values.Select(pl => 4 + pl.Sum(p => p.SizeInBytes)).Sum();

            var ms = new MemoryStream();
            request.WriteTo(ms);
            byte[] bytes = ms.ToArray();
            Assert.IsNotNull(bytes);

            // add 4 bytes for the length of the message at the beginning
            Assert.AreEqual(requestSize, bytes.Length);

            // first 4 bytes = the message length
            Assert.AreEqual(requestSize - 4, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the request type
            Assert.AreEqual((short) RequestTypes.Fetch,
                            BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the version id
            Assert.AreEqual((short) FetchRequest.CurrentVersion,
                            BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the correlation id
            Assert.AreEqual(correlationId,
                            BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(8).Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the client id length
            Assert.AreEqual((short) clientId.Length,
                            BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(12).Take(2).ToArray<byte>()), 0));

            // next few bytes = the client id
            Assert.AreEqual(clientId, Encoding.ASCII.GetString(bytes.Skip(14).Take(clientId.Length).ToArray<byte>()));

            // next 4 bytes = replica id
            Assert.AreEqual(-1,
                            BitConverter.ToInt32(
                                BitWorks.ReverseBytes(bytes.Skip(14 + clientId.Length).Take(4).ToArray<byte>()), 0));

            // next 4 bytes = max wait
            Assert.AreEqual(maxWait,
                            BitConverter.ToInt32(
                                BitWorks.ReverseBytes(bytes.Skip(18 + clientId.Length).Take(4).ToArray<byte>()), 0));

            // next 4 bytes = min bytes
            Assert.AreEqual(minBytes,
                            BitConverter.ToInt32(
                                BitWorks.ReverseBytes(bytes.Skip(22 + clientId.Length).Take(4).ToArray<byte>()), 0));

            // next 4 bytes = offset info count
            Assert.AreEqual(1,
                            BitConverter.ToInt32(
                                BitWorks.ReverseBytes(bytes.Skip(26 + clientId.Length).Take(4).ToArray<byte>()), 0));

            //=== offset info part

            // next 2 bytes = the topic length
            Assert.AreEqual((short) topicName.Length,
                            BitConverter.ToInt16(
                                BitWorks.ReverseBytes(bytes.Skip(30 + clientId.Length).Take(2).ToArray<byte>()), 0));

            // next few bytes = the topic
            Assert.AreEqual(topicName,
                            Encoding.ASCII.GetString(
                                bytes.Skip(32 + clientId.Length).Take(topicName.Length).ToArray<byte>()));

            // next 4 bytes = partitions count
            Assert.AreEqual(1,
                            BitConverter.ToInt32(
                                BitWorks.ReverseBytes(
                                    bytes.Skip(32 + clientId.Length + topicName.Length).Take(4).ToArray<byte>()), 0));

            // next 4 bytes = partition
            Assert.AreEqual(2,
                            BitConverter.ToInt32(
                                BitWorks.ReverseBytes(
                                    bytes.Skip(36 + clientId.Length + topicName.Length).Take(4).ToArray<byte>()), 0));

            // next 4 bytes = offset
            Assert.AreEqual(4000,
                            BitConverter.ToInt64(
                                BitWorks.ReverseBytes(
                                    bytes.Skip(40 + clientId.Length + topicName.Length).Take(8).ToArray<byte>()), 0));

            // next 4 bytes = fetch size
            Assert.AreEqual(777,
                            BitConverter.ToInt32(
                                BitWorks.ReverseBytes(
                                    bytes.Skip(48 + clientId.Length + topicName.Length).Take(8).ToArray<byte>()), 0));
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void FetchRequestBuilderMostlyDefault()
        {
            var builder = new FetchRequestBuilder();
            var topic = "topic";
            var partitionId = 1;
            long offset = 2;
            var fetchSize = 3;
            var request = builder.AddFetch(topic, partitionId, offset, fetchSize).Build();

            Assert.IsNotNull(request);
            Assert.AreEqual(FetchRequest.CurrentVersion, request.VersionId);
            Assert.AreEqual(-1, request.CorrelationId);
            Assert.AreEqual(string.Empty, request.ClientId);
            Assert.AreEqual(-1, request.ReplicaId);
            Assert.AreEqual(-1, request.MaxWait);
            Assert.AreEqual(-1, request.MinBytes);
            Assert.AreEqual(1, request.OffsetInfo.Count());

            Assert.AreEqual(topic, request.OffsetInfo.First().Key);
            Assert.AreEqual(partitionId, request.OffsetInfo.First().Value.ToArray()[0].PartitionId);
            Assert.AreEqual(offset, request.OffsetInfo.First().Value.ToArray()[0].Offset);
            Assert.AreEqual(fetchSize, request.OffsetInfo.First().Value.ToArray()[0].FetchSize);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void FetchRequestBuilderCustom()
        {
            var builder = new FetchRequestBuilder();
            var topic = "topic";
            var partitionId = 1;
            long offset = 2;
            var fetchSize = 3;

            var correlationId = 4;
            var clientId = "myClient";
            var maxWait = 400;
            var minBytes = 500;

            var request =
                builder.AddFetch(topic, partitionId, offset, fetchSize).CorrelationId(correlationId).ClientId(clientId).
                        MaxWait(maxWait).MinBytes(minBytes).Build();

            Assert.IsNotNull(request);
            Assert.AreEqual(FetchRequest.CurrentVersion, request.VersionId);
            Assert.AreEqual(correlationId, request.CorrelationId);
            Assert.AreEqual(clientId, request.ClientId);
            Assert.AreEqual(maxWait, request.MaxWait);
            Assert.AreEqual(minBytes, request.MinBytes);
            Assert.AreEqual(1, request.OffsetInfo.Count());

            Assert.AreEqual(topic, request.OffsetInfo.First().Key);
            Assert.AreEqual(partitionId, request.OffsetInfo.First().Value.ToArray()[0].PartitionId);
            Assert.AreEqual(offset, request.OffsetInfo.First().Value.ToArray()[0].Offset);
            Assert.AreEqual(fetchSize, request.OffsetInfo.First().Value.ToArray()[0].FetchSize);
        }
    }
}
