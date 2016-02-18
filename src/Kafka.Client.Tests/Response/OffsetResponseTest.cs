// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Response
{
    using FluentAssertions;
    using Kafka.Client.Consumers;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using TestHelper;

    [TestClass]
    public class OffsetResponseTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldParseResponse()
        {
            var stream = new MemoryStream();
            var writer = new KafkaBinaryWriter(stream);
            writer.Write(1);
            writer.Write(123); // correlation id
            writer.Write(1); // topic count
            writer.WriteShortString("topic");
            writer.Write(1); // partition count
            writer.Write(999); // partition id
            writer.Write((short)ErrorMapping.NoError);
            writer.Write(3); // number of offsets
            writer.Write(111L);
            writer.Write(222L);
            writer.Write(333L);
            stream.Seek(0, SeekOrigin.Begin);
            var reader = new KafkaBinaryReader(stream);
            var response = new OffsetResponse.Parser().ParseFrom(reader);
            response.CorrelationId.Should().Be(123);
            response.ResponseMap.Count.Should().Be(1);
            var partitions = response.ResponseMap["topic"];
            partitions.Count.Should().Be(1);
            var info = partitions.First();
            info.Error.Should().Be(ErrorMapping.NoError);
            info.Offsets.Count.Should().Be(3);
            info.Offsets.SequenceEqual(new List<long>() {111L, 222L, 333L}).Should().BeTrue();
            info.PartitionId.Should().Be(999);
        }
    }
}
