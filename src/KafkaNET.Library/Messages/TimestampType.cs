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
    using System;
    using System.Globalization;

    public static class TimestampType
    {
        public static TimestampTypes GetTimestampType(int timeStampType)
        {
            switch (timeStampType)
            {
                case 0:
                    return TimestampTypes.CreateTime;
                case 8:
                    return TimestampTypes.LogAppendTime;
                default:
                    throw new UnknownCodecException(String.Format(
                        CultureInfo.CurrentCulture,
                        "{0} is an unknown timestamp type",
                        timeStampType));
            }
        }

        public static byte GetTimestampTypeValue(TimestampTypes timestampType)
        {
            switch (timestampType)
            {
                case TimestampTypes.NoTimestamp:
                case TimestampTypes.CreateTime:
                    return (byte)0;
                case TimestampTypes.LogAppendTime:
                    return (byte)8;
                default:
                    throw new UnknownCodecException(String.Format(
                        CultureInfo.CurrentCulture,
                        "{0} is an unknown timestamp type",
                        timestampType));
            }
        }
    }
}
