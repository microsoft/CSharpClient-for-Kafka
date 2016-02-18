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
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Requests;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using TestHelper;

    [TestClass]
    public class TopicMetadataRequestTests
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToParseRequest()
        {
            var stream = new MemoryStream();
            var writer = new KafkaBinaryWriter(stream);
            writer.Write(1);
            writer.Write(100); // correlation id
            writer.Write(2); // broker count
            writer.Write(0); // broker id 
            writer.WriteShortString("host1");
            writer.Write(9092); // port
            writer.Write(1); // broker id 
            writer.WriteShortString("host2");
            writer.Write(9093); // port
            writer.Write(1); // topic count
            writer.Write((short)ErrorMapping.NoError);
            writer.WriteShortString("topic1");
            writer.Write(1); // partitions
            writer.Write((short)ErrorMapping.NoError);
            writer.Write(111); // partition id
            writer.Write(0); // leader broker id
            writer.Write(1); // num replicas
            writer.Write(1); // replica broker id
            writer.Write(1); // in sync replicas
            writer.Write(1); // in sync replica broker id
            stream.Seek(0, SeekOrigin.Begin);
            var reader = new KafkaBinaryReader(stream);
            var response = new TopicMetadataRequest.Parser().ParseFrom(reader);
            var enumerator = response.GetEnumerator();
            enumerator.MoveNext().Should().BeTrue();
            enumerator.Current.Topic.Should().Be("topic1");
            enumerator.Current.Error.Should().Be(ErrorMapping.NoError);
            var partitionEnumerator = enumerator.Current.PartitionsMetadata.GetEnumerator();
            partitionEnumerator.MoveNext().Should().BeTrue();
            partitionEnumerator.Current.PartitionId.Should().Be(111);
            var leader = partitionEnumerator.Current.Leader;            
            leader.Id.Should().Be(0);
            leader.Host.Should().Be("host1");
            leader.Port.Should().Be(9092);
            var replicas = partitionEnumerator.Current.Replicas.ToList();
            replicas.Count.Should().Be(1);
            replicas.First().Id.Should().Be(1);
            replicas.First().Host.Should().Be("host2");
            replicas.First().Port.Should().Be(9093);
            var isrs = partitionEnumerator.Current.Isr.ToList();
            isrs.Count.Should().Be(1);
            isrs.First().Id.Should().Be(1);
            isrs.First().Host.Should().Be("host2");
            isrs.First().Port.Should().Be(9093);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void TopicMetadataRequestWithNoSegmentMetadataCreation()
        {
            var topics = new List<string> { "topic1", "topic2" };

            var request = TopicMetadataRequest.Create(topics, 1, 1, "test");

            Assert.IsNotNull(request);
            Assert.AreEqual(topics.Count, request.Topics.Count());

            for (int i = 0; i < topics.Count; i++)
            {
                var expectedTopic = topics[i];
                var actualTopic = request.Topics.ElementAt(i);

                Assert.AreEqual(expectedTopic, actualTopic);
            }

            Assert.AreEqual(DetailedMetadataRequest.NoSegmentMetadata, request.DetailedMetadata);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        [TestCategory(TestCategories.BVT)]
        public void TopicMetadataRequestShouldThrowExceptionWhenListOfTopicsIsNull()
        {
            IList<string> topics = null;

            TopicMetadataRequest.Create(topics, 1, 1, "test");
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        [TestCategory(TestCategories.BVT)]
        public void TopicMetadataRequestShouldThrowExceptionWhenListOfTopicsIsEmpty()
        {
            IList<string> topics = new List<string>();

            TopicMetadataRequest.Create(topics, 1, 1, "test");
        }
    }
}