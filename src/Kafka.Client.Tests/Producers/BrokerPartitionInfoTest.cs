// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Producers
{
    using FluentAssertions;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using TestHelper;

    [TestClass]
    public class BrokerPartitionInfoTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void UpdateInfoUpdatesCache()
        {
            var pool = new Mock<ISyncProducerPool>();
            var producer = new Mock<ISyncProducer>();
            var partitionMetadatas = new List<PartitionMetadata>()
            {
                new PartitionMetadata(0, new Broker(0, "host1", 1234), Enumerable.Empty<Broker>(),
                    Enumerable.Empty<Broker>())
            };
            var metadatas = new List<TopicMetadata>() { new TopicMetadata("test", partitionMetadatas, ErrorMapping.NoError) };
            producer.Setup(p => p.Send(It.IsAny<TopicMetadataRequest>())).Returns(() => metadatas);
            pool.Setup(p => p.GetShuffledProducers()).Returns(() => new List<ISyncProducer>() { producer.Object });
            var cache = new Dictionary<string, TopicMetadata>();
            var info = new BrokerPartitionInfo(pool.Object, cache, new Dictionary<string, DateTime> { { "test", DateTime.MinValue } }, 1, null);
            info.GetBrokerPartitionInfo(1, "test", 1, "test");
            cache.ContainsKey("test").Should().BeTrue();
            var data = cache["test"];
            data.PartitionsMetadata.Count().Should().Be(1);
            data.PartitionsMetadata.First().Leader.Id.Should().Be(0);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldUpdateInfoWhenBrokerNotFound()
        {
            var pool = new Mock<ISyncProducerPool>();
            var producer = new Mock<ISyncProducer>();
            var partitionMetadatas = new List<PartitionMetadata>()
            {
                new PartitionMetadata(0, new Broker(0, "host1", 1234), Enumerable.Empty<Broker>(),
                    Enumerable.Empty<Broker>())
            };
            var metadatas = new List<TopicMetadata>() { new TopicMetadata("test", partitionMetadatas, ErrorMapping.NoError) };
            producer.Setup(p => p.Send(It.IsAny<TopicMetadataRequest>())).Returns(() => metadatas);
            pool.Setup(p => p.GetShuffledProducers()).Returns(() => new List<ISyncProducer>() { producer.Object });
            var info = new BrokerPartitionInfo(pool.Object);
            var partitions = info.GetBrokerPartitionInfo(1, "test", 1, "test");
            partitions.Count.Should().Be(1);
            var partition = partitions.First();
            partition.Topic.Should().Be("test");
            partition.Leader.BrokerId.Should().Be(0);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        [ExpectedException(typeof(KafkaException))]
        public void ShouldNotUpdateInfoWhenErrorResponse()
        {
            var pool = new Mock<ISyncProducerPool>();
            var producer = new Mock<ISyncProducer>();
            var partitionMetadatas = new List<PartitionMetadata>()
            {
                new PartitionMetadata(0, new Broker(0, "host1", 1234), Enumerable.Empty<Broker>(),
                    Enumerable.Empty<Broker>())
            };
            var metadatas = new List<TopicMetadata>() { new TopicMetadata("test", partitionMetadatas, ErrorMapping.LeaderNotAvailableCode) };
            producer.Setup(p => p.Send(It.IsAny<TopicMetadataRequest>())).Returns(() => metadatas);

            var producerConfig = new SyncProducerConfiguration(new ProducerConfiguration((IList<BrokerConfiguration>)null), 0, "host1", 1234);
            producer.Setup(p => p.Config).Returns(producerConfig);
            pool.Setup(p => p.GetShuffledProducers()).Returns(() => new List<ISyncProducer>() { producer.Object });
            var info = new BrokerPartitionInfo(pool.Object);
            info.GetBrokerPartitionInfo(1, "test", 1, "test");
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldUpdateInfoWhenCachedErrorResponse()
        {
            var pool = new Mock<ISyncProducerPool>();
            var producer = new Mock<ISyncProducer>();
            var partitionMetadatas = new List<PartitionMetadata>()
            {
                new PartitionMetadata(0, new Broker(0, "host1", 1234), Enumerable.Empty<Broker>(),
                    Enumerable.Empty<Broker>())
            };
            var cachedPartitionMetadatas = new List<PartitionMetadata>()
            {
                new PartitionMetadata(1, new Broker(1, "host1", 1234), Enumerable.Empty<Broker>(),
                    Enumerable.Empty<Broker>())
            };
            var metadatas = new List<TopicMetadata>() { new TopicMetadata("test", partitionMetadatas, ErrorMapping.NoError) };
            producer.Setup(p => p.Send(It.IsAny<TopicMetadataRequest>())).Returns(() => metadatas);
            pool.Setup(p => p.GetShuffledProducers()).Returns(() => new List<ISyncProducer>() { producer.Object });
            var cache = new Dictionary<string, TopicMetadata>();
            cache["test"] = new TopicMetadata("test", cachedPartitionMetadatas, ErrorMapping.NotLeaderForPartitionCode);
            var info = new BrokerPartitionInfo(pool.Object, cache, new Dictionary<string, DateTime> { { "test", DateTime.MinValue } }, 1, null);
            var partitions = info.GetBrokerPartitionInfo(1, "test", 1, "test");
            partitions.Count.Should().Be(1);
            var partition = partitions.First();
            partition.Topic.Should().Be("test");
            partition.Leader.BrokerId.Should().Be(0);
        }
    }
}
