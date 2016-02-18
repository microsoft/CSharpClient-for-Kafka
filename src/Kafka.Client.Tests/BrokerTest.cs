// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests
{
    using FluentAssertions;
    using Kafka.Client.Cluster;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using TestHelper;

    [TestClass]
    public class BrokerTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToCreateBroker()
        {
            var broker = new Broker(1, "host1", 1234);
            broker.Id.Should().Be(1);
            broker.Host.Should().Be("host1");
            broker.Port.Should().Be(1234);
            broker.SizeInBytes.Should().Be(BitWorks.GetShortStringLength("host1", "UTF-8") + 8);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToParseBroker()
        {
            var broker = Broker.CreateBroker(1, @"{'host': 'host1', 'port': 1234}");
            broker.Id.Should().Be(1);
            broker.Host.Should().Be("host1");
            broker.Port.Should().Be(1234);
            broker.SizeInBytes.Should().Be(BitWorks.GetShortStringLength("host1", "UTF-8") + 8);
        }
    }
}
