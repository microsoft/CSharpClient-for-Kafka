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

    public struct ByteBuffer
    {
        byte[] buffer;
        uint offset;
        readonly int length;

        public static ByteBuffer NewAsync(byte[] buffer)
        {
            return new ByteBuffer(buffer, 0, buffer.Length);
        }

        public static ByteBuffer NewAsync(byte[] buffer, int offset, int length)
        {
            return new ByteBuffer(buffer, (uint)offset, length);
        }

        public static ByteBuffer NewSync(byte[] buffer)
        {
            return new ByteBuffer(buffer, 0x80000000u, buffer.Length);
        }

        public static ByteBuffer NewSync(byte[] buffer, int offset, int length)
        {
            return new ByteBuffer(buffer, (((uint)offset) | 0x80000000u), length);
        }

        public static ByteBuffer NewEmpty()
        {
            return new ByteBuffer(BitArrayManipulation.EmptyByteArray, 0, 0);
        }

        private ByteBuffer(byte[] buffer, uint offset, int length)
        {
            this.buffer = buffer;
            this.offset = offset;
            this.length = length;
        }

        public byte[] Buffer { get { return this.buffer; } }
        public int Offset { get { return (int)(this.offset & 0x7fffffffu); } }
        public int Length { get { return this.length; } }
        public bool AsyncSafe { get { return (this.offset & 0x80000000u) == 0u; } }

        public ByteBuffer ToAsyncSafe()
        {
            if (AsyncSafe) return this;
            var copy = new byte[this.length];
            Array.Copy(this.buffer, Offset, copy, 0, this.length);
            return NewAsync(copy);
        }

        public void MakeAsyncSafe()
        {
            if (AsyncSafe) return;
            var copy = new byte[this.length];
            Array.Copy(this.buffer, Offset, copy, 0, this.length);
            this.buffer = copy;
            this.offset = 0;
        }

        public ByteBuffer ResizingAppend(ByteBuffer append)
        {
            if (AsyncSafe)
            {
                if (Offset + Length + append.Length <= Buffer.Length)
                {
                    Array.Copy(append.Buffer, append.Offset, Buffer, Offset + Length, append.Length);
                    return NewAsync(Buffer, Offset, Length + append.Length);
                }
            }
            var newCapacity = Math.Max(Length + append.Length, Length * 2);
            var newBuffer = new byte[newCapacity];
            Array.Copy(Buffer, Offset, newBuffer, 0, Length);
            Array.Copy(append.Buffer, append.Offset, newBuffer, Length, append.Length);
            return NewAsync(newBuffer, 0, Length + append.Length);
        }

        internal ArraySegment<byte> ToArraySegment()
        {
            return new ArraySegment<byte>(Buffer, Offset, Length);
        }

        internal byte[] ToByteArray()
        {
            var safeSelf = ToAsyncSafe();
            var buf = safeSelf.Buffer ?? BitArrayManipulation.EmptyByteArray;
            if (safeSelf.Offset == 0 && safeSelf.Length == buf.Length)
            {
                return buf;
            }
            var copy = new byte[safeSelf.Length];
            Array.Copy(safeSelf.Buffer, safeSelf.Offset, copy, 0, safeSelf.Length);
            return copy;
        }

        internal MemoryStream ToStream()
        {
            return new MemoryStream(this.ToByteArray());
        }
    }
}
