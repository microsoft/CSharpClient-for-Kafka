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
        public void ShouldAbleToParseFetchResponse()
        {
            var stream = new MemoryStream();
            var writer = new KafkaBinaryWriter(stream);
            writer.Write(1);
            writer.Write(123); // correlation id
            writer.Write(1); // data count
            writer.WriteShortString("topic1");
            writer.Write(1); // partition count
            writer.Write(111); //partition id
            writer.Write((short)ErrorMapping.NoError);

            writer.Write(1011L); // hw            
            var messageStream = new MemoryStream();
            var messageWriter = new KafkaBinaryWriter(messageStream);
            new BufferedMessageSet(new List<Message>() { new Message(new byte[100]) }, 0).WriteTo(messageWriter);
            writer.Write((int)messageStream.Length);
            writer.Write(messageStream.GetBuffer(), 0, (int)messageStream.Length);
            stream.Seek(0, SeekOrigin.Begin);
            var reader = new KafkaBinaryReader(stream);
            var response = new FetchResponse.Parser().ParseFrom(reader);
            var set = response.MessageSet("topic1", 111);
            set.Should().NotBeNull();
            var messages = set.Messages.ToList();
            messages.Count().Should().Be(1);
            messages.First().Payload.Length.Should().Be(100);
        }
    }
}
