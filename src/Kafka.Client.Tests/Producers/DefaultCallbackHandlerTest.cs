// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Producers
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using Kafka.Client.Responses;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using System.Collections.Generic;
    using System.Linq;
    using TestHelper;

    [TestClass]
    public class DefaultCallbackHandlerTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldHandleEvents()
        {
            var partitioner = new Mock<IPartitioner<string>>();
            var config = new ProducerConfiguration(new List<BrokerConfiguration>());
            var pool = new Mock<ISyncProducerPool>();
            var producer = new Mock<ISyncProducer>();
            var partitionMetadatas = new List<PartitionMetadata>()
            {
                new PartitionMetadata(0, new Broker(0, "host1", 1234), Enumerable.Empty<Broker>(),
                    Enumerable.Empty<Broker>())
            };
            var metadatas = new List<TopicMetadata>() { new TopicMetadata("test", partitionMetadatas, ErrorMapping.NoError) };
            producer.SetupGet(p => p.Config)
                    .Returns(
                        () =>
                            new SyncProducerConfiguration(new ProducerConfiguration(new List<BrokerConfiguration>()), 0,
                                "host1", 1234));
            producer.Setup(p => p.Send(It.IsAny<TopicMetadataRequest>())).Returns(() => metadatas);
            var statuses = new Dictionary<TopicAndPartition, ProducerResponseStatus>();
            statuses[new TopicAndPartition("test", 0)] = new ProducerResponseStatus();
            producer.Setup(p => p.Send(It.IsAny<ProducerRequest>()))
                    .Returns(
                        () =>
                            new ProducerResponse(1, statuses));
            pool.Setup(p => p.GetShuffledProducers()).Returns(() => new List<ISyncProducer>() { producer.Object });
            pool.Setup(p => p.GetProducer(It.IsAny<int>())).Returns(() => producer.Object);
            var mockPartitionInfo = new Mock<IBrokerPartitionInfo>();
            mockPartitionInfo.Setup(m => m.GetBrokerPartitionInfo(0, string.Empty, It.IsAny<int>(), "test"))
                             .Returns(() =>
                             {
                                 var partition = new Partition("test", 0);
                                 var replica = new Replica(0, "test");
                                 partition.Leader = replica;
                                 return new List<Partition>() { partition };
                             });
            var handler = new DefaultCallbackHandler<string, Message>(config, partitioner.Object, new DefaultEncoder(), mockPartitionInfo.Object, pool.Object);
            handler.Handle(new List<ProducerData<string, Message>>() { new ProducerData<string, Message>("test", new Message(new byte[100])) });
            pool.Verify(p => p.GetProducer(0));
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldRetryHandleWhenTopicNotFound()
        {
            var partitioner = new Mock<IPartitioner<string>>();
            var config = new ProducerConfiguration(new List<BrokerConfiguration>());
            var pool = new Mock<ISyncProducerPool>();
            var producer = new Mock<ISyncProducer>();
            var partitionMetadatas = new List<PartitionMetadata>()
            {
                new PartitionMetadata(0, new Broker(0, "host1", 1234), Enumerable.Empty<Broker>(),
                    Enumerable.Empty<Broker>())
            };
            var metadatas = new List<TopicMetadata>() { new TopicMetadata("test", partitionMetadatas, ErrorMapping.NoError) };
            producer.SetupGet(p => p.Config)
                    .Returns(
                        () =>
                            new SyncProducerConfiguration(new ProducerConfiguration(new List<BrokerConfiguration>()), 0,
                                "host1", 1234));
            producer.Setup(p => p.Send(It.IsAny<TopicMetadataRequest>())).Returns(() => metadatas);
            var statuses = new Dictionary<TopicAndPartition, ProducerResponseStatus>();
            statuses[new TopicAndPartition("test", 0)] = new ProducerResponseStatus();
            producer.Setup(p => p.Send(It.IsAny<ProducerRequest>()))
                    .Returns(
                        () =>
                            new ProducerResponse(1, statuses));
            pool.Setup(p => p.GetShuffledProducers()).Returns(() => new List<ISyncProducer>() { producer.Object });
            pool.Setup(p => p.GetProducer(It.IsAny<int>())).Returns(() => producer.Object);
            var mockPartitionInfo = new Mock<IBrokerPartitionInfo>();
            mockPartitionInfo.Setup(m => m.GetBrokerPartitionInfo(0, string.Empty, It.IsAny<int>(), "test"))
                             .Returns(() => new List<Partition>());
            var handler = new DefaultCallbackHandler<string, Message>(config, partitioner.Object, new DefaultEncoder(), mockPartitionInfo.Object, pool.Object);
            try
            {
                handler.Handle(new List<ProducerData<string, Message>>()
                {
                    new ProducerData<string, Message>("test", new Message(new byte[100]))
                });
            }
            catch (FailedToSendMessageException<string>)
            {
                mockPartitionInfo.Verify(m => m.GetBrokerPartitionInfo(0, string.Empty, It.IsAny<int>(), "test"), Times.Exactly(3));
                return;
            }

            Assert.Fail("Should have caught exception.");
        }
    }
}
