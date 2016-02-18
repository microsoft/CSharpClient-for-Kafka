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

    public static class SnappyCompress
    {
        const int MaxOffset = 1 << 15;

        public static int Compress(ByteBuffer dstBuf, ByteBuffer srcBuf)
        {
            var src = srcBuf.Buffer;
            var dst = dstBuf.Buffer;
            var s = srcBuf.Offset;
            var d = dstBuf.Offset;
            var sL = srcBuf.Length;
            var dL = dstBuf.Length;
            if (dL < 5) return -1;
            EmitLength(dst, ref d, ref dL, sL);
            if (sL <= 4)
            {
                if (!EmitLiteral(dst, ref d, ref dL, src, s, sL)) return -1;
                return d - dstBuf.Offset;
            }

            var shift = 32 - 8;
            var tableSize = 1 << 8;
            while (tableSize < 1 << 14 && tableSize < sL)
            {
                shift--;
                tableSize *= 2;
            }

            var table = new int[tableSize];
            for (int i = 0; i < tableSize; i++)
            {
                table[i] = -1;
            }

            var lit = s;
            while (sL > 3)
            {
                var v = src[s] | ((uint)src[s + 1]) << 8 | ((uint)src[s + 2]) << 16 | ((uint)src[s + 3]) << 24;
                nextfast:
                var h = (v * 0x1e35a7bd) >> shift;
                var t = table[h];
                table[h] = s;
                if (t < 0 || s - t >= MaxOffset || !Equal4(src, t, s))
                {
                    s++;
                    sL--;
                    if (sL > 3)
                    {
                        v = (v >> 8) | ((uint)src[s + 3]) << 24;
                        goto nextfast;
                    }
                    break;
                }

                if (lit != s)
                {
                    if (!EmitLiteral(dst, ref d, ref dL, src, lit, s - lit)) return -1;
                }

                var s0 = s;
                s += 4;
                sL -= 4;
                t += 4;
                while (sL > 0 && src[s] == src[t])
                {
                    s++;
                    sL--;
                    t++;
                }

                if (!EmitCopy(dst, ref d, ref dL, s - t, s - s0)) return -1;
                lit = s;
            }

            s += sL;
            if (lit != s)
            {
                if (!EmitLiteral(dst, ref d, ref dL, src, lit, s - lit)) return -1;
            }

            return d - dstBuf.Offset;
        }

        public static bool TryCompress(ref ByteBuffer data, int maxSizeInPercent)
        {
            var compressed = ByteBuffer.NewAsync(new byte[data.Length * (long)maxSizeInPercent / 100]);
            var compressedLength = Compress(compressed, data);
            if (compressedLength < 0) return false;
            data = ByteBuffer.NewAsync(compressed.Buffer, 0, compressedLength);
            return true;
        }

        static bool Equal4(byte[] buf, int o1, int o2)
        {
            return buf[o1] == buf[o2] &&
                   buf[o1 + 1] == buf[o2 + 1] &&
                   buf[o1 + 2] == buf[o2 + 2] &&
                   buf[o1 + 3] == buf[o2 + 3];
        }

        static bool EmitLiteral(byte[] dst, ref int d, ref int dL, byte[] src, int s, int sL)
        {
            if (sL < 61)
            {
                if (sL + 1 > dL) return false;
                dst[d] = (byte)((sL - 1) << 2);
                d++;
                dL--;
            }
            else if (sL <= 0x100)
            {
                if (sL + 2 > dL) return false;
                dst[d] = 60 << 2;
                dst[d + 1] = (byte)(sL - 1);
                d += 2;
                dL -= 2;
            }
            else if (sL <= 0x10000)
            {
                if (sL + 3 > dL) return false;
                dst[d] = 61 << 2;
                dst[d + 1] = (byte)(sL - 1);
                dst[d + 2] = (byte)((sL - 1) >> 8);
                d += 3;
                dL -= 3;
            }
            else if (sL <= 0x1000000)
            {
                if (sL + 4 > dL) return false;
                dst[d] = 62 << 2;
                dst[d + 1] = (byte)(sL - 1);
                dst[d + 2] = (byte)((sL - 1) >> 8);
                dst[d + 3] = (byte)((sL - 1) >> 16);
                d += 4;
                dL -= 4;
            }
            else
            {
                if (sL + 5 > dL) return false;
                dst[d] = 63 << 2;
                dst[d + 1] = (byte)(sL - 1);
                dst[d + 2] = (byte)((sL - 1) >> 8);
                dst[d + 3] = (byte)((sL - 1) >> 16);
                dst[d + 4] = (byte)((sL - 1) >> 24);
                d += 5;
                dL -= 5;
            }

            Array.Copy(src, s, dst, d, sL);
            d += sL;
            dL -= sL;
            return true;
        }

        static bool EmitCopy(byte[] dst, ref int d, ref int dL, int offset, int length)
        {
            while (length > 0)
            {
                var x = length - 4;
                if (0 <= x && x < 8 && offset < 1 << 11)
                {
                    if (dL < 2) return false;
                    dst[d] = (byte)((offset >> 3) & 0xe0 | (x << 2) | 1);
                    dst[d + 1] = (byte)offset;
                    d += 2;
                    dL -= 2;
                    break;
                }

                x = length;
                if (x > 1 << 6)
                {
                    x = 1 << 6;
                }
                if (dL < 3) return false;
                dst[d] = (byte)((x - 1) << 2 | 2);
                dst[d + 1] = (byte)offset;
                dst[d + 2] = (byte)(offset >> 8);
                d += 3;
                dL -= 3;
                length -= x;
            }

            return true;
        }

        static void EmitLength(byte[] dst, ref int d, ref int dL, int length)
        {
            if (length < 0x80)
            {
                dst[d] = (byte)length;
                d++;
                dL--;
            }
            else if (length < 0x4000)
            {
                dst[d] = (byte)(length | 128);
                dst[d + 1] = (byte)(length >> 7);
                d += 2;
                dL -= 2;
            }
            else if (length < 0x200000)
            {
                dst[d] = (byte)(length | 128);
                dst[d + 1] = (byte)((length >> 7) | 128);
                dst[d + 2] = (byte)(length >> 14);
                d += 3;
                dL -= 3;
            }
            else if (length < 0x10000000)
            {
                dst[d] = (byte)(length | 128);
                dst[d + 1] = (byte)((length >> 7) | 128);
                dst[d + 2] = (byte)((length >> 14) | 128);
                dst[d + 3] = (byte)(length >> 21);
                d += 4;
                dL -= 4;
            }
            else
            {
                dst[d] = (byte)(length | 128);
                dst[d + 1] = (byte)((length >> 7) | 128);
                dst[d + 2] = (byte)((length >> 14) | 128);
                dst[d + 3] = (byte)((length >> 21) | 128);
                dst[d + 4] = (byte)(length >> 28);
                d += 5;
                dL -= 5;
            }
        }
    }
}