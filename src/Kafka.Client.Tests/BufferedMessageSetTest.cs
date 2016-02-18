// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests
{
    using FluentAssertions;
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using TestHelper;

    [TestClass]
    public class BufferedMessageSetTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToEnumerateMessages()
        {
            var msg1 = new Message(new byte[101]) {Offset = 0};
            var msg2 = new Message(new byte[102]) {Offset = 1};
            var set = new BufferedMessageSet(new List<Message>() {msg1, msg2}, 0);
            set.MoveNext().Should().BeTrue();
            set.Current.Message.Payload.Length.Should().Be(101);
            set.Current.Message.Offset.Should().Be(0);
            set.MoveNext().Should().BeTrue();
            set.Current.Message.Payload.Length.Should().Be(102);
            set.Current.Message.Offset.Should().Be(1);
            set.MoveNext().Should().BeFalse();
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldParseEmptyMessageSet()
        {
            var stream = new MemoryStream();
            var reader = new KafkaBinaryReader(stream);
            var newSet = BufferedMessageSet.ParseFrom(reader, 0, 0);
            var messages = newSet.Messages.ToList();
            messages.Count().Should().Be(0);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToWriteMessageSetWithExtraBytes()
        {            
            var stream = new MemoryStream();
            var writer = new KafkaBinaryWriter(stream);
            var msg1 = new Message(new byte[101]) {Offset = 0};
            var msg2 = new Message(new byte[102]) {Offset = 1};
            var set = new BufferedMessageSet(new List<Message>() {msg1, msg2}, 0);
            set.WriteTo(writer);
            writer.Write(new byte[10]); // less than offset and size
            var size = (int) stream.Position;
            stream.Seek(0, SeekOrigin.Begin);
            var reader = new KafkaBinaryReader(stream);
            var newSet = BufferedMessageSet.ParseFrom(reader, size, 0);
            var messages = newSet.Messages.ToList();
            messages.Count().Should().Be(2);
            messages[0].Payload.Count().Should().Be(101);
            messages[1].Payload.Count().Should().Be(102);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToWriteMessageSetWithPartialMessage()
        {
            var stream = new MemoryStream();
            var writer = new KafkaBinaryWriter(stream);
            var msg1 = new Message(new byte[101]) { Offset = 0 };
            var msg2 = new Message(new byte[102]) { Offset = 1 };
            var set = new BufferedMessageSet(new List<Message>() { msg1, msg2 }, 0);
            set.WriteTo(writer);
            // Writing partial message 
            writer.Write(3L); 
            writer.Write(100);
            writer.Write(new byte[10]);
            var size = (int)stream.Position;
            stream.Seek(0, SeekOrigin.Begin);
            var reader = new KafkaBinaryReader(stream);
            var newSet = BufferedMessageSet.ParseFrom(reader, size, 0);
            var messages = newSet.Messages.ToList();
            messages.Count().Should().Be(2);
            messages[0].Payload.Count().Should().Be(101);
            messages[1].Payload.Count().Should().Be(102);
        }
    }
}
