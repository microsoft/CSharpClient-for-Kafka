// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Response
{
    using FluentAssertions;
    using Kafka.Client.Responses;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.IO;
    using TestHelper;

    [TestClass]
    public class ProducerResponseTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToParseResponse()
        {
            var stream = new MemoryStream();
            var writer = new KafkaBinaryWriter(stream);
            writer.Write(1);
            writer.Write(123); // correlation id
            writer.Write(1); // topic count
            writer.WriteShortString("topic");
            writer.Write(1); // partition count
            writer.Write(999); // partition id
            writer.Write((short)ErrorMapping.NoError); // error
            writer.Write(111L); // offset
            stream.Seek(0, SeekOrigin.Begin);
            var reader = new KafkaBinaryReader(stream);
            var response = new ProducerResponse.Parser().ParseFrom(reader);
            response.CorrelationId.Should().Be(123);
            response.Statuses.Count.Should().Be(1);
            var info = response.Statuses[new TopicAndPartition("topic", 999)];
            info.Error.Should().Be(ErrorMapping.NoError);
            info.Offset.Should().Be(111L);
        }
    }
}
