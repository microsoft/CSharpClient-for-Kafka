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

namespace Kafka.Client.Messages.Compression
{
    using System;
    using System.Diagnostics;

    internal static class BitArrayManipulation
    {
        public static readonly byte[] EmptyByteArray = new byte[0];
        static readonly byte[] FirstHoleSize = new byte[]
                                                           {
                                                               8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0,
                                                               2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1,
                                                               0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0,
                                                               1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2,
                                                               0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
                                                               4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1,
                                                               0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0,
                                                               1, 0, 3, 0, 1, 0, 2, 0, 1, 0
                                                           };

        static readonly byte[] LastHoleSize = new byte[]
                                                          {
                                                              8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2,
                                                              2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1,
                                                              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                                              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                              0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                                                          };

        static readonly byte[] MaxHoleSize = new byte[]
                                                         {
                                                             8, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 5, 4, 3, 3,
                                                             2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 6, 5, 4, 4, 3, 3, 3,
                                                             3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2,
                                                             1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 7, 6, 5, 5, 4, 4, 4, 4, 3, 3, 3, 3, 3,
                                                             3, 3, 3, 4, 3, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1,
                                                             4, 3, 2, 2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 6, 5, 4, 4, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 4, 3, 2,
                                                             2, 2, 1, 1, 1, 3, 2, 1, 1, 2, 1, 1, 1, 5, 4, 3, 3, 2, 2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 1, 4, 3, 2, 2, 2, 1,
                                                             1, 1, 3, 2, 1, 1, 2, 1, 1, 0
                                                         };

        static readonly byte[] MaxHoleOffset = new byte[]
                                                           {
                                                               0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 5, 5, 5, 5, 5, 5, 0, 5, 5, 5, 5, 5, 5, 5, 0, 1, 2, 2,
                                                               0, 3, 3, 3, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 0, 6, 6, 6, 0, 1, 6, 6, 0, 6, 6, 6, 0, 1, 2, 2, 3, 3, 3,
                                                               3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1,
                                                               0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 2, 2, 3, 3, 3, 3, 0, 4, 4, 4, 4,
                                                               4, 4, 4, 0, 1, 2, 2, 0, 5, 5, 5, 0, 1, 5, 5, 0, 5, 5, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4,
                                                               0, 1, 2, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 2, 2, 3, 3, 3, 3, 0, 1, 4, 4, 0, 4, 4, 4, 0, 1, 2,
                                                               2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 2, 2, 0, 3, 3, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 2, 2, 0, 1,
                                                               0, 3, 0, 1, 0, 2, 0, 1, 0, 0
                                                           };

        internal static int IndexOfFirstHole(byte[] data, int size, int startOffset)
        {
            Debug.Assert(size > 0);
            Debug.Assert(startOffset >= 0);
            var pos = (uint)(startOffset / 8);
            var firstByteFill = (byte)(255 >> (8 - (startOffset & 7)));
            uint sizetill = 0;
            uint laststart = pos * 8;
            int len = data.Length;
            if (pos >= len) return -1;
            var b = (uint)(data[pos] | firstByteFill);
            pos++;
            switch (b)
            {
                case 255:
                    laststart += 8;
                    break;
                case 0:
                    sizetill += 8;
                    break;
                default:
                    if (FirstHoleSize[b] >= size) return (int)laststart;
                    if (MaxHoleSize[b] >= size) return (int)(pos * 8 + MaxHoleOffset[b] - 8);
                    sizetill = LastHoleSize[b];
                    laststart += 8 - sizetill;
                    break;
            }

            while (pos < len)
            {
                b = data[pos];
                pos++;
                switch (b)
                {
                    case 255:
                        if (sizetill >= size) return (int)laststart;
                        sizetill = 0;
                        laststart = pos * 8;
                        break;
                    case 0:
                        sizetill += 8;
                        break;
                    default:
                        sizetill += FirstHoleSize[b];
                        if (sizetill >= size) return (int)laststart;
                        if (MaxHoleSize[b] >= size) return (int)(pos * 8 + MaxHoleOffset[b] - 8);
                        sizetill = LastHoleSize[b];
                        laststart = pos * 8 - sizetill;
                        break;
                }
            }

            return sizetill >= size ? (int)laststart : -1;
        }

        internal static void SetBits(byte[] data, int position, int size)
        {
            Debug.Assert(position >= 0 && size > 0 && position + size <= data.Length * 8);
            var startMask = (byte)~(255 >> (8 - (position & 7)));
            int startBytePos = position / 8;
            var endMask = (byte)(255 >> (7 - ((position + size - 1) & 7)));
            int endBytePos = (position + size - 1) / 8;
            if (startBytePos == endBytePos)
            {
                data[startBytePos] |= (byte)(startMask & endMask);
            }
            else
            {
                data[startBytePos] |= startMask;
                startBytePos++;
                while (startBytePos < endBytePos)
                {
                    data[startBytePos] = 255;
                    startBytePos++;
                }

                data[endBytePos] |= endMask;
            }
        }

        internal static void UnsetBits(byte[] data, int position, int size)
        {
            Debug.Assert(position >= 0 && size > 0 && position + size <= data.Length * 8);
            var startMask = (byte)~(255 >> (8 - (position & 7)));
            int startBytePos = position / 8;
            var endMask = (byte)(255 >> (7 - ((position + size - 1) & 7)));
            int endBytePos = (position + size - 1) / 8;
            if (startBytePos == endBytePos)
            {
                data[startBytePos] &= (byte)~(startMask & endMask);
            }
            else
            {
                data[startBytePos] &= (byte)~startMask;
                startBytePos++;
                while (startBytePos < endBytePos)
                {
                    data[startBytePos] = 0;
                    startBytePos++;
                }

                data[endBytePos] &= (byte)~endMask;
            }
        }

        internal static int CompareByteArray(byte[] a1, int o1, int l1, byte[] a2, int o2, int l2)
        {
            var commonLength = Math.Min(l1, l2);
            for (var i = 0; i < commonLength; i++)
            {
                var b1 = a1[o1 + i];
                var b2 = a2[o2 + i];
                if (b1 < b2) return -1;
                if (b1 > b2) return 1;
            }

            if (l1 < l2) return -1;
            if (l1 > l2) return 1;
            return 0;
        }
    }
}