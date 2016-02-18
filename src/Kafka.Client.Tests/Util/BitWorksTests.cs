/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Kafka.Client.Tests.Util
{
    using FluentAssertions;
    using Kafka.Client.Serialization;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.IO;
    using TestHelper;
    using Utils;

    /// <summary>
    /// Tests for <see cref="BitWorks"/> utility class.
    /// </summary>
    [TestClass]
    public class BitWorksTests
    {
        /// <summary>
        /// Ensures bytes are returned reversed.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void GetBytesReversedShortValid()
        {
            short val = 100;
            byte[] normal = BitConverter.GetBytes(val);
            byte[] reversed = BitWorks.GetBytesReversed(val);

            TestReversedArray(normal, reversed);
        }

        /// <summary>
        /// Ensures bytes are returned reversed.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void GetBytesReversedIntValid()
        {
            int val = 100;
            byte[] normal = BitConverter.GetBytes(val);
            byte[] reversed = BitWorks.GetBytesReversed(val);

            TestReversedArray(normal, reversed);
        }

        /// <summary>
        /// Ensures bytes are returned reversed.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void GetBytesReversedLongValid()
        {
            long val = 100L;
            byte[] normal = BitConverter.GetBytes(val);
            byte[] reversed = BitWorks.GetBytesReversed(val);

            TestReversedArray(normal, reversed);
        }

        /// <summary>
        /// Null array will reverse to a null.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ReverseBytesNullArray()
        {
            byte[] arr = null;
            Assert.IsNull(BitWorks.ReverseBytes(arr));
        }

        /// <summary>
        /// Zero length array will reverse to a zero length array.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ReverseBytesZeroLengthArray()
        {
            byte[] arr = new byte[0];
            byte[] reversedArr = BitWorks.ReverseBytes(arr);
            Assert.IsNotNull(reversedArr);
            Assert.AreEqual(0, reversedArr.Length);
        }

        /// <summary>
        /// Array is reversed.
        /// </summary>
        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ReverseBytesValid()
        {
            byte[] arr = BitConverter.GetBytes((short)1);
            byte[] original = new byte[2];
            arr.CopyTo(original, 0);
            byte[] reversedArr = BitWorks.ReverseBytes(arr);

            TestReversedArray(original, reversedArr);
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ReverseInt()
        {
            TestReversedArray(BitConverter.GetBytes(1234), BitWorks.GetBytesReversed(1234));
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ReverseLong()
        {
            TestReversedArray(BitConverter.GetBytes(1234L), BitWorks.GetBytesReversed(1234L));
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ReverseShort()
        {
            TestReversedArray(BitConverter.GetBytes((short)1111), BitWorks.GetBytesReversed((short)1111));
        }

        [TestMethod]
        [TestCategory(TestCategories.BVT)]
        public void ShouldReadShortString()
        {
            var stream = new MemoryStream();
            var writer = new KafkaBinaryWriter(stream);
            writer.WriteShortString("hello world");
            stream.Seek(0, SeekOrigin.Begin);
            BitWorks.ReadShortString(new KafkaBinaryReader(stream), "UTF-8").Should().Be("hello world");
        }

        /// <summary>
        /// Performs asserts for two arrays that should be exactly the same, but values
        /// in one are in reverse order of the other.
        /// </summary>
        /// <param name="normal">The "normal" array.</param>
        /// <param name="reversed">The array that is in reverse order to the "normal" one.</param>
        private static void TestReversedArray(byte[] normal, byte[] reversed)
        {
            Assert.IsNotNull(reversed);
            Assert.AreEqual(normal.Length, reversed.Length);
            for (int ix = 0; ix < normal.Length; ix++)
            {
                Assert.AreEqual(normal[ix], reversed[reversed.Length - 1 - ix]);
            }
        }
    }
}
