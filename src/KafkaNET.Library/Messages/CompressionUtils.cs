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



namespace Kafka.Client.Messages
{
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages.Compression;
    using Kafka.Client.Serialization;
    using log4net;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Reflection;
    using Utils;

    public static class CompressionUtils
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(CompressionUtils));

        public static Message Compress(IEnumerable<Message> messages, int partition)
        {
            return Compress(messages, CompressionCodecs.DefaultCompressionCodec, partition);
        }

        public static Message Compress(IEnumerable<Message> messages, CompressionCodecs compressionCodec, int partition)
        {
            switch (compressionCodec)
            {
                case CompressionCodecs.DefaultCompressionCodec:
                case CompressionCodecs.GZIPCompressionCodec:
                    using (var outputStream = new MemoryStream())
                    {
                        using (var gZipStream = new GZipStream(outputStream, CompressionMode.Compress))
                        {
                            if (Logger.IsDebugEnabled)
                            {
                                Logger.DebugFormat(
                                    "Allocating BufferedMessageSet of size = {0}",
                                    MessageSet.GetMessageSetSize(messages));
                            }

                            var bufferedMessageSet = new BufferedMessageSet(messages, partition);
                            using (var inputStream = new MemoryStream(bufferedMessageSet.SetSize))
                            {
                                bufferedMessageSet.WriteTo(inputStream);
                                inputStream.Position = 0;
                                try
                                {
                                    gZipStream.Write(inputStream.ToArray(), 0, inputStream.ToArray().Length);
                                    gZipStream.Close();
                                }
                                catch (IOException ex)
                                {
                                    Logger.ErrorFormat("Error while writing to the GZIP stream {0}", ex.FormatException());
                                    throw;
                                }
                            }

                            Message oneCompressedMessage = new Message(outputStream.ToArray(), compressionCodec)
                            {
                                PartitionId = partition
                            };
                            return oneCompressedMessage;
                        }
                    }

                case CompressionCodecs.SnappyCompressionCodec:
                    Logger.DebugFormat(
                            "Allocating BufferedMessageSet of size = {0}",
                            MessageSet.GetMessageSetSize(messages));

                    var messageSet = new BufferedMessageSet(messages, partition);
                    using (var inputStream = new MemoryStream(messageSet.SetSize))
                    {
                        messageSet.WriteTo(inputStream);
                        inputStream.Position = 0;

                        try
                        {
                            return new Message(SnappyHelper.Compress(inputStream.GetBuffer()), compressionCodec)
                            {
                                PartitionId = partition
                            };
                        }
                        catch (Exception ex)
                        {
                            Logger.ErrorFormat("Error while writing to the Snappy stream {0}", ex.FormatException());
                            throw;
                        }
                    }

                default:
                    throw new UnknownCodecException(String.Format(CultureInfo.CurrentCulture, "Unknown Codec: {0}", compressionCodec));
            }
        }

        public static BufferedMessageSet Decompress(Message message, int partition)
        {
            switch (message.CompressionCodec)
            {
                case CompressionCodecs.DefaultCompressionCodec:
                case CompressionCodecs.GZIPCompressionCodec:
                    byte[] inputBytes = message.Payload;
                    using (var outputStream = new MemoryStream())
                    {
                        using (var inputStream = new MemoryStream(inputBytes))
                        {
                            using (var gzipInputStream = new GZipStream(inputStream, CompressionMode.Decompress))
                            {
                                try
                                {
                                    gzipInputStream.CopyTo(outputStream);
                                    gzipInputStream.Close();
                                }
                                catch (IOException ex)
                                {
                                    Logger.InfoFormat("Error while reading from the GZIP input stream: {0}", ex.FormatException());
                                    throw;
                                }
                            }
                        }

                        outputStream.Position = 0;
                        using (var reader = new KafkaBinaryReader(outputStream))
                        {
                            return BufferedMessageSet.ParseFrom(reader, (int)outputStream.Length, partition);
                        }
                    }

                case CompressionCodecs.SnappyCompressionCodec:
                    try
                    {
                        using (var stream = new MemoryStream(SnappyHelper.Decompress(message.Payload)))
                        {
                            using (var reader = new KafkaBinaryReader(stream))
                            {
                                return BufferedMessageSet.ParseFrom(reader, (int)stream.Length, partition);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.ErrorFormat("Error while reading from the Snappy input stream  {0}", ex.FormatException());
                        throw;
                    }

                default:
                    throw new UnknownCodecException(String.Format(CultureInfo.CurrentCulture, "Unknown Codec: {0}", message.CompressionCodec));
            }
        }
    }
}
