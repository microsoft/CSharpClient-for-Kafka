// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Response
{
    using FluentAssertions;
    using Kafka.Client.Messages;
    using Kafka.Client.Responses;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using TestHelper;

    [TestClass]
    public class FetchResponseTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToParseV0FetchResponse()
        {
            var stream = new MemoryStream();
            WriteTestFetchResponse(stream, 0);
            var reader = new KafkaBinaryReader(stream);
            var response = new FetchResponse.Parser(0).ParseFrom(reader);
            response.ThrottleTime.ShouldBeEquivalentTo(0);
            var set = response.MessageSet("topic1", 111);
            set.Should().NotBeNull();
            var messages = set.Messages.ToList();
            messages.Count().Should().Be(1);
            messages.First().Payload.Length.Should().Be(100);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToParseV1FetchResponse()
        {
            var stream = new MemoryStream();
            WriteTestFetchResponse(stream, 1);
            var reader = new KafkaBinaryReader(stream);
            var response = new FetchResponse.Parser(1).ParseFrom(reader);
            response.ThrottleTime.ShouldBeEquivalentTo(456);
            var set = response.MessageSet("topic1", 111);
            set.Should().NotBeNull();
            var messages = set.Messages.ToList();
            messages.Count().Should().Be(1);
            messages.First().Payload.Length.Should().Be(100);
        }

        private static void WriteTestFetchResponse(MemoryStream stream, int versionId)
        {
            var writer = new KafkaBinaryWriter(stream);
            writer.Write(1);
            writer.Write(123); // correlation id
            if (versionId > 0)
            {
               writer.Write(456); // throttle time
            }
            writer.Write(1); // data count
            writer.WriteShortString("topic1");
            writer.Write(1); // partition count
            writer.Write(111); //partition id
            writer.Write((short) ErrorMapping.NoError);

            writer.Write(1011L); // hw            
            var messageStream = new MemoryStream();
            var messageWriter = new KafkaBinaryWriter(messageStream);
            new BufferedMessageSet(new List<Message>() {new Message(new byte[100])}, 0).WriteTo(messageWriter);
            writer.Write((int) messageStream.Length);
            writer.Write(messageStream.GetBuffer(), 0, (int) messageStream.Length);
            stream.Seek(0, SeekOrigin.Begin);
        }
    }
}
