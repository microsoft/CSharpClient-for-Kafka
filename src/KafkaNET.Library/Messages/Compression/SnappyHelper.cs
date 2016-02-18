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

    public static class SnappyHelper
    {
        public static byte[] Compress(byte[] input)
        {
            var compressionArray = new byte[input.Length + 2];
            var compressedLength = SnappyCompress.Compress(ByteBuffer.NewSync(compressionArray), ByteBuffer.NewSync(input));

            var output = new byte[compressedLength];

            Array.Copy(compressionArray, output, compressedLength);

            return output;
        }

        public static byte[] Decompress(byte[] input)
        {
            var buffer = SnappyDecompress.Decompress(ByteBuffer.NewSync(input));
            return buffer.ToByteArray();
        }
    }
}
