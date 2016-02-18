// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Response
{
    using FluentAssertions;
    using Kafka.Client.Responses;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using TestHelper;

    [TestClass]
    public class TopicAndPartitionTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldHoldTopicAndPartition()
        {
            var tp = new TopicAndPartition("testTopic", 1);
            tp.PartitionId.Should().Be(1);
            tp.Topic.Should().Be("testTopic");
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void TopicAndPartitionShouldEqual()
        {
            var tp = new TopicAndPartition("testTopic", 1);
            var tp2 = new TopicAndPartition("testTopic", 1);

            tp.Equals(tp2).Should().BeTrue();
            tp.Equals(new TopicAndPartition("testTopic2", 1)).Should().BeFalse();
        }
    }
}
