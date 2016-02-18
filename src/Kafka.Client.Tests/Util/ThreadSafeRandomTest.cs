// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Tests.Util
{
    using FluentAssertions;
    using Kafka.Client.Utils;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using TestHelper;

    [TestClass]
    public class ThreadSafeRandomTest
    {
        static readonly ThreadSafeRandom Random = new ThreadSafeRandom();
        private static readonly List<int> R1 = new List<int>();
        private static readonly List<int> R2 = new List<int>();

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldBeThreadSafeInNext()
        {
            R1.Clear();
            R2.Clear();
            var t1 = new Thread(GetNextR1);
            var t2 = new Thread(GetNextR2);
            t1.Start();
            t2.Start();
            t1.Join();
            t2.Join();
            R1.SequenceEqual(R2).Should().BeFalse();
        }

        private void GetNextR1()
        {
            for(int i = 0; i < 100; ++i)
                R1.Add(Random.Next());
        }

        private void GetNextR2()
        {
            for (int i = 0; i < 100; ++i)
                R2.Add(Random.Next());
        }
        
    }
}
