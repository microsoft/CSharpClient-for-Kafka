// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Exceptions
{
    using FluentAssertions;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using TestHelper;

    [TestClass]
    public class KafkaExceptionTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldGetCorrectErrorCode()
        {
            foreach (var code in Enum.GetNames(typeof (ErrorMapping)))
            {
                var errorMapping = (ErrorMapping)Enum.Parse(typeof(ErrorMapping), code);
                new KafkaException(errorMapping).ErrorCode.Should().Be((short)errorMapping);
            }
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToGetRightMessage()
        {
            new KafkaException(ErrorMapping.LeaderNotAvailableCode).Message.Should()
                                                                   .Be("Leader not found for given topic/partition.");
            new KafkaException(ErrorMapping.OffsetOutOfRangeCode).Message.Should()
                                                                   .Be("Offset out of range.");
            new KafkaException(ErrorMapping.InvalidMessageCode).Message.Should()
                                                                   .Be("Invalid message.");
            new KafkaException(ErrorMapping.UnknownTopicOrPartitionCode).Message.Should()
                                                                   .Be("Unknown topic or partition.");
            new KafkaException(ErrorMapping.UnknownCode).Message.Should()
                                                                   .Be("Unknown Error.");
        }
    }
}
