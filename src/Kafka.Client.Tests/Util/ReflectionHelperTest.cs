// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Util
{
    using FluentAssertions;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using TestHelper;

    [TestClass]
    public class ReflectionHelperTest
    {
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToInstantiateClass()
        {
            ReflectionHelper.Instantiate<DefaultEncoder>("Kafka.Client.Serialization.DefaultEncoder")
                .GetType().Should().Be(typeof(DefaultEncoder));
        }

        [TestMethod]
        [ExpectedException(typeof(TypeLoadException))]
        [TestCategory(TestCategories.BVT)]
        public void ShouldThrowExceptionWhenInvalidClassInstantiated()
        {
            ReflectionHelper.Instantiate<DefaultEncoder>("Kafka.Client.Serialization.DefaultEncoder2");
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldAbleToInstantiateDefault()
        {
            ReflectionHelper.Instantiate<DefaultEncoder>(string.Empty).Should().BeNull();
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        [TestCategory(TestCategories.BVT)]
        public void ShouldThrowWhenGenericTypeDiffers()
        {
            ReflectionHelper.Instantiate<StringEncoder>("Kafka.Client.Serialization.DefaultEncoder");
        }


        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldInstantiateGenericType()
        {
            var partitioner =
                ReflectionHelper.Instantiate<IPartitioner<string>>("Kafka.Client.Producers.Partitioning.DefaultPartitioner`1");
            partitioner.GetType().Should().Be(new DefaultPartitioner<string>().GetType());
        }
    }
}
