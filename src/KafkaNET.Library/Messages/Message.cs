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

using System.Security.Cryptography;

namespace Kafka.Client.Messages
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;

    /// <summary>
    /// Message send to Kafka server
    /// </summary>
    /// <remarks>
    /// Format:
    /// 1 byte "magic" identifier to allow format changes
    /// 4 byte CRC32 of the payload
    /// N - 5 byte payload
    /// </remarks>
    public class Message : IWritable
    {
        public const int DefaultHeaderSize = DefaultMagicLength + DefaultCrcLength + DefaultAttributesLength + DefaultKeySizeLength + DefaultValueSizeLength;

        private const byte DefaultMagicValue = 0;


        /// <summary>
        /// Need set magic to 1 while compress,
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Wire+Format for detail
        /// </summary>
        private const byte MagicValueWhenCompress = 1;
        private const byte DefaultMagicLength = 1;
        private const byte DefaultCrcLength = 4;
        private const byte MagicOffset = DefaultCrcLength;
        private const byte DefaultAttributesLength = 1;
        private const byte DefaultKeySizeLength = 4;
        private const byte DefaultValueSizeLength = 4;
        private const byte CompressionCodeMask = 3;

        private long _offset = -1;

        /// <summary>
        /// Initializes a new instance of the <see cref="Message"/> class.
        /// </summary>
        /// <param name="payload">
        /// The payload.
        /// </param>
        /// <remarks>
        /// Initializes the magic number as default and the checksum as null. It will be automatically computed.
        /// </remarks>
        public Message(byte[] payload)
            : this(payload, null, CompressionCodecs.NoCompressionCodec)
        {
            Guard.NotNull(payload, "payload");
        }

        public Message(byte[] payload, CompressionCodecs compressionCodec)
            : this(payload, null, compressionCodec)
        {
            Guard.NotNull(payload, "payload");
        }

        /// <summary>
        /// Initializes a new instance of the Message class.
        /// </summary>
        /// <param name="payload">The data for the payload.</param>
        /// <param name="magic">The magic identifier.</param>
        /// <param name="checksum">The checksum for the payload.</param>
        public Message(byte[] payload, byte[] key, CompressionCodecs compressionCodec)
        {
            Guard.NotNull(payload, "payload");

            int length = DefaultHeaderSize + payload.Length;
            Key = key;
            if (key != null)
            {
                length += key.Length;
            }

            this.Payload = payload;
            this.Magic = DefaultMagicValue;
            if (compressionCodec != CompressionCodecs.NoCompressionCodec)
            {
                this.Attributes |=
                    (byte)(CompressionCodeMask & Messages.CompressionCodec.GetCompressionCodecValue(compressionCodec));

                // It seems that the java producer uses magic 0 for compressed messages, so we are sticking with 0 for now
                // this.Magic = MagicValueWhenCompress;
            }

            this.Size = length;
        }

        /// <summary>
        /// Gets the payload.
        /// </summary>
        public byte[] Payload { get; private set; }

        /// <summary>
        /// Gets the magic bytes.
        /// </summary>
        public byte Magic { get; private set; }

        /// <summary>
        /// Gets the Attributes for the message.
        /// </summary>
        public byte Attributes { get; private set; }

        /// <summary>
        /// Gets the total size of message.
        /// </summary>
        public int Size { get; private set; }

        public byte[] Key { get; private set; }

        public long Offset { get { return _offset; } set { _offset = value; } }

        /// <summary>
        /// When produce data, do not need set this field.
        /// When consume data, need set this field.
        /// </summary>
        public int? PartitionId { get; set; }

        public int KeyLength
        {
            get
            {
                if (Key == null)
                {
                    return -1;
                }
                else
                {
                    return Key.Length;
                }
            }
        }

        /// <summary>
        /// Gets the payload size.
        /// </summary>
        public int PayloadSize
        {
            get
            {
                return this.Payload.Length;
            }
        }

        public CompressionCodecs CompressionCodec
        {
            get
            {
                return Messages.CompressionCodec.GetCompressionCodec(Attributes & CompressionCodeMask);
            }
        }

        /// <summary>
        /// Writes message data into given message buffer
        /// </summary>
        /// <param name="output">
        /// The output.
        /// </param>
        public void WriteTo(MemoryStream output)
        {
            Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                this.WriteTo(writer);
            }
        }

        /// <summary>
        /// Writes message data using given writer
        /// </summary>
        /// <param name="writer">
        ///     The writer.
        /// </param>
        /// <param name="getBuffer"></param>
        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");
            writer.Seek(MagicOffset, SeekOrigin.Current);
            var beginningPosition = writer.CurrentPos;
            writer.Write(this.Magic);
            writer.Write(this.Attributes);
            writer.Write(this.KeyLength);
            if (KeyLength != -1)
            {
                writer.Write(this.Key);
            }
            writer.Write(Payload.Length);
            writer.Write(this.Payload);
            var crc = ComputeChecksum(writer.Buffer, (int)beginningPosition, Size - MagicOffset);
            writer.Seek(-Size, SeekOrigin.Current);
            writer.Write(crc);
            writer.Seek(Size - DefaultCrcLength, SeekOrigin.Current);
        }

        /// <summary>
        /// Try to show the payload as decoded to UTF-8.
        /// </summary>
        /// <returns>The decoded payload as string.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("Magic: ");
            sb.Append(this.Magic);
            if (this.Magic == 1)
            {
                sb.Append(", Attributes: ");
                sb.Append(this.Attributes);
            }

            sb.Append(", topic: ");
            try
            {
                sb.Append(Encoding.UTF8.GetString(this.Payload));
            }
            catch (Exception)
            {
                sb.Append("n/a");
            }

            return sb.ToString();
        }

        /**
        * A message. The format of an N byte message is the following:
        *
        * 1. 4 byte CRC32 of the message
        * 2. 1 byte "magic" identifier to allow format changes, value is 2 currently
        * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
        * 4. 4 byte key length, containing length K
        * 5. K byte key
        * 6. 4 byte payload length, containing length V
        * 7. V byte payload
        *
        */
        internal static Message ParseFrom(KafkaBinaryReader reader, long offset, int size, int partitionID)
        {
            Message result;
            int readed = 0;
            uint checksum = reader.ReadUInt32();
            readed += 4;
            byte magic = reader.ReadByte();
            readed++;

            byte[] payload;
            if (magic == 2 || magic == 0) // some producers (CLI) send magic 0 while others have value of 2
            {
                byte attributes = reader.ReadByte();
                readed++;
                var keyLength = reader.ReadInt32();
                readed += 4;
                byte[] key = null;
                if (keyLength != -1)
                {
                    key = reader.ReadBytes(keyLength);
                    readed += keyLength;
                }
                var payloadSize = reader.ReadInt32();
                readed += 4;
                payload = reader.ReadBytes(payloadSize);
                readed += payloadSize;
                result = new Message(payload, key, Messages.CompressionCodec.GetCompressionCodec(attributes & CompressionCodeMask))
                {
                    Offset = offset,
                    PartitionId = partitionID
                };
            }
            else
            {
                payload = reader.ReadBytes(size - DefaultHeaderSize);
                readed += size - DefaultHeaderSize;
                result = new Message(payload) { Offset = offset, PartitionId = partitionID };
            }

            if (size != readed)
            {
                throw new KafkaException(ErrorMapping.InvalidFetchSizeCode);
            }

            return result;
        }

        /// <summary>
        /// Clean up attributes for message, otherwise there is double decompress at kafka broker side.
        /// </summary>
        internal void CleanMagicAndAttributesBeforeCompress()
        {
            this.Attributes = 0;
            this.Magic = DefaultMagicValue;
        }

        /// <summary>
        /// Restore the Magic and Attributes after compress.
        /// </summary>
        /// <param name="magic"></param>
        /// <param name="attributes"></param>
        internal void RestoreMagicAndAttributesAfterCompress(byte magic, byte attributes)
        {
            this.Attributes = attributes;
            this.Magic = magic;
        }

        private uint ComputeChecksum(byte[] message, int offset, int count)
        {
            return Crc32Hasher.ComputeCrcUint32(message, offset, count);
        }
    }
}
