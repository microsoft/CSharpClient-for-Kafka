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

namespace Kafka.Client.Requests
{
    using System;
    using System.IO;
    using System.Text;

    /// <summary>
    /// Base request to make to Kafka.
    /// </summary>
    public abstract class AbstractRequest
    {
        public const string DefaultEncoding = "UTF-8";
        public const short DefaultTopicLengthIfNonePresent = 2;

        protected const byte DefaultRequestSizeSize = 4;
        protected const byte DefaultRequestIdSize = 2;

        public MemoryStream RequestBuffer { get; protected set; }

        public abstract RequestTypes RequestType { get; }

        protected short RequestTypeId
        {
            get
            {
                return (short)this.RequestType;
            }
        }

        protected static short GetTopicLength(string topic, string encoding = DefaultEncoding)
        {
            Encoding encoder = Encoding.GetEncoding(encoding);
            return string.IsNullOrEmpty(topic) ? DefaultTopicLengthIfNonePresent : (short)encoder.GetByteCount(topic);
        }

        protected short GetShortStringLength(string text, string encoding = DefaultEncoding)
        {
            if (string.IsNullOrEmpty(text))
            {
                return (short)0;
            }
            else
            {
                Encoding encoder = Encoding.GetEncoding(encoding);
                return (short)encoder.GetByteCount(text);
            }
        }

        public short GetShortStringWriteLength(string text, string encoding = DefaultEncoding)
        {
            return (short)(2 + GetShortStringLength(text, encoding));
        }
    }
}
