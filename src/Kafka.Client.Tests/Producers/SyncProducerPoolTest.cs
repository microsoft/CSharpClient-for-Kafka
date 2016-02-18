// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Producers
{
    using FluentAssertions;
    using Kafka.Client.Cfg;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Producers.Sync;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using System.Collections.Generic;
    using System.Linq;
    using TestHelper;

    [TestClass]
    public class SyncProducerPoolTest
    {
        private SyncProducerPool pool;

        [TestInitialize]
        [TestCategory(TestCategories.BVT)]
        public void Setup()
        {
            var broker1 = new Mock<ISyncProducer>();
            broker1.SetupGet(b => b.Config).Returns(() => new SyncProducerConfiguration()
            {
                BrokerId = 1
            });
            var broker2 = new Mock<ISyncProducer>();
            broker2.SetupGet(b => b.Config).Returns(() => new SyncProducerConfiguration()
            {
                BrokerId = 2
            });
            var brokers = new List<ISyncProducer> { broker1.Object, broker2.Object };
            pool = new SyncProducerPool(new ProducerConfiguration((List<BrokerConfiguration>)null), brokers);
        }

        [TestMethod]
        [ExpectedException(typeof(UnavailableProducerException))]
        [TestCategory(TestCategories.BVT)]
        public void ShouldThrowWhenBrokerDoesNotExist()
        {
            pool.GetProducer(3);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldGetProducerById()
        {
            pool.GetProducer(1).Config.BrokerId.Should().Be(1);
            pool.GetProducer(2).Config.BrokerId.Should().Be(2);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldGetShuffledProducers()
        {
            var brokers = new HashSet<int> { 1, 2 };
            pool.GetShuffledProducers().All(p => brokers.Remove(p.Config.BrokerId)).Should().BeTrue();
        }


    }
}
