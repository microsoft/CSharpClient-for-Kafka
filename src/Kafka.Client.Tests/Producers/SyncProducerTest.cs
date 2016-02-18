// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Producers
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using System.Collections.Generic;
    using TestHelper;

    [TestClass]
    public class SyncProducerTest
    {
        [TestMethod]
        [ExpectedException(typeof(MessageSizeTooLargeException))]
        [TestCategory(TestCategories.BVT)]
        public void ShouldThrowMessageTooLarge()
        {
            var connection = new Mock<IKafkaConnection>();
            var config = new SyncProducerConfiguration { MaxMessageSize = 99 };
            var producer = new SyncProducer(config, connection.Object);
            producer.Send(new ProducerRequest(1, "client", 0, 0, new List<TopicData>()
            {
                new TopicData("test",
                    new List<PartitionData>()
                    {
                        new PartitionData(0, new BufferedMessageSet(new List<Message>() {new Message(new byte[100])}, 0))
                    })
            }));
        }
    }
}
