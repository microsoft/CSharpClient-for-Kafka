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
    using FluentAssertions;
    using Kafka.Client.Serialization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Requests;
    using System.Collections.Generic;
    using System.IO;
    using TestHelper;
    using Utils;

    /// <summary>
    /// Tests the <see cref="OffsetRequest"/> class.
    /// </summary>
    [TestClass]
    public class OffsetRequestTests
    {
        /// <summary>
        /// Validates the list of bytes meet Kafka expectations.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void GetBytesValid()
        {
            const string topicName = "topic";
            var requestInfo = new Dictionary<string, List<PartitionOffsetRequestInfo>>();
            requestInfo[topicName] = new List<PartitionOffsetRequestInfo>() { new PartitionOffsetRequestInfo(0, OffsetRequest.LatestTime, 10) };
            var request = new OffsetRequest(requestInfo);

            // format = len(request) + requesttype + version + correlation id + client id + replica id + request info count + request infos
            int count = 2 + 2 + 4 + 2 + 4 + 4 + 4 +
                        BitWorks.GetShortStringLength("topic", AbstractRequest.DefaultEncoding) + 4 + 4 + 8 + 4;
            var ms = new MemoryStream();
            request.WriteTo(ms);
            byte[] bytes = ms.ToArray();
            Assert.IsNotNull(bytes);
            Assert.AreEqual(count, bytes.Length);

            var reader = new KafkaBinaryReader(ms);
            reader.ReadInt32().Should().Be(count - 4); // length
            reader.ReadInt16().Should().Be((short)RequestTypes.Offsets); // request type
            reader.ReadInt16().Should().Be(0); // version
            reader.ReadInt32().Should().Be(0); // correlation id
            string.IsNullOrEmpty(reader.ReadShortString()).Should().BeTrue(); // client id
            reader.ReadInt32().Should().Be(-1); // replica id
            reader.ReadInt32().Should().Be(1); // request info count
            reader.ReadShortString().Should().Be("topic");
            reader.ReadInt32().Should().Be(1); // info count
            reader.ReadInt32().Should().Be(0); // partition id
            reader.ReadInt64().Should().Be(OffsetRequest.LatestTime); // time
            reader.ReadInt32().Should().Be(10); // max offset
        }
    }
}
