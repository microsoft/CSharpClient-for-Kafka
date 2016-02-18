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
    using System.IO;

    public static class SnappyDecompress
    {
        public static int DecompressedSize(ByteBuffer compressedBytes, out int length)
        {
            var offset = compressedBytes.Offset;
            var limit = offset + compressedBytes.Length;
            var buf = compressedBytes.Buffer;
            if (offset >= limit) goto error;
            var b = buf[offset];
            offset++;
            var result = (uint)(b & 127);
            if (b < 128) goto done;
            if (offset >= limit) goto error;
            b = buf[offset];
            offset++;
            result |= ((uint)(b & 127) << 7);
            if (b < 128) goto done;
            if (offset >= limit) goto error;
            b = buf[offset];
            offset++;
            result |= ((uint)(b & 127) << 14);
            if (b < 128) goto done;
            if (offset >= limit) goto error;
            b = buf[offset];
            offset++;
            result |= ((uint)(b & 127) << 21);
            if (b < 128) goto done;
            if (offset >= limit) goto error;
            b = buf[offset];
            offset++;
            result |= ((uint)(b & 127) << 28);
            if (b >= 16) goto error;
        done:
            length = offset - compressedBytes.Offset;
            return (int)result;
        error:
            length = 0;
            return -1;
        }

        public static ByteBuffer Decompress(ByteBuffer compressedBytes)
        {
            int ofs;
            var decompressedSize = DecompressedSize(compressedBytes, out ofs);
            if (decompressedSize < 0) throw new InvalidDataException();
            var dst = new byte[decompressedSize];
            var dstBuf = ByteBuffer.NewAsync(dst);
            if (!DecompressRaw(dstBuf, ByteBuffer.NewSync(compressedBytes.Buffer, compressedBytes.Offset + ofs, compressedBytes.Length - ofs)))
                throw new InvalidDataException();
            return dstBuf;
        }

        public static bool DecompressRaw(ByteBuffer dstBuf, ByteBuffer srcBuf)
        {
            var src = srcBuf.Buffer;
            var dst = dstBuf.Buffer;
            var s = srcBuf.Offset;
            var d = dstBuf.Offset;
            var sL = srcBuf.Length;
            var dL = dstBuf.Length;
            int len = 0;
            int o = 0;
            while (sL > 0)
            {
                var b = src[s];
                s++;
                sL--;
                switch (b & 3)
                {
                    case 0:
                        len = b >> 2;
                        if (len < 60)
                        {
                            len++;
                        }
                        else if (len == 60)
                        {
                            if (sL < 1) return false;
                            len = src[s] + 1;
                            s++;
                            sL--;
                        }
                        else if (len == 61)
                        {
                            if (sL < 2) return false;
                            len = src[s] + 0x100 * src[s + 1] + 1;
                            s += 2;
                            sL -= 2;
                        }
                        else if (len == 62)
                        {
                            if (sL < 3) return false;
                            len = src[s] + 0x100 * src[s + 1] + 0x10000 * src[s + 2] + 1;
                            s += 3;
                            sL -= 3;
                        }
                        else
                        {
                            if (sL < 4) return false;
                            len = src[s] + 0x100 * src[s + 1] + 0x10000 * src[s + 2] + 0x1000000 * src[s + 3] + 1;
                            s += 3;
                            sL -= 3;
                        }

                        if (len <= 0) return false;
                        if (len > dL || len > sL) return false;
                        Array.Copy(src, s, dst, d, len);
                        s += len;
                        d += len;
                        sL -= len;
                        dL -= len;
                        continue;
                    case 1:
                        if (sL < 1) return false;
                        len = 4 + ((b >> 2) & 7);
                        o = (b & 0xe0) << 3 | src[s];
                        s++;
                        sL--;
                        break;
                    case 2:
                        if (sL < 2) return false;
                        len = 1 + (b >> 2);
                        o = src[s] + src[s + 1] * 0x100;
                        s += 2;
                        sL -= 2;
                        break;
                    case 3:
                        return false;
                }

                var end = d + len;
                if (o > d || len > dL)
                    return false;
                for (; d < end; d++)
                {
                    dst[d] = dst[d - o];
                }
                dL -= len;
            }

            return dL == 0;
        }
    }
}