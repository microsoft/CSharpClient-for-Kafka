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

namespace Kafka.Client.Utils
{
    using System;
    using System.Globalization;

    /// <summary>
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public enum ErrorMapping : short
    {
        UnknownCode = -1,
        NoError = 0,
        OffsetOutOfRangeCode = 1,
        InvalidMessageCode = 2,
        UnknownTopicOrPartitionCode = 3,
        InvalidFetchSizeCode = 4,
        LeaderNotAvailableCode = 5,
        NotLeaderForPartitionCode = 6,
        RequestTimedOutCode = 7,
        BrokerNotAvailableCode = 8,
        ReplicaNotAvailableCode = 9,
        MessagesizeTooLargeCode = 10,
        StaleControllerEpochCode = 11,
        OffsetMetadataTooLargeCode = 12,
        StaleLeaderEpochCode = 13,
        OffsetsLoadInProgressCode = 14,
        ConsumerCoordinatorNotAvailableCode = 15,
        NotCoordinatorForConsumerCode = 16
    }

    public static class ErrorMapper
    {
        public static ErrorMapping ToError(short error)
        {
            return (ErrorMapping)Enum.Parse(typeof(ErrorMapping), error.ToString(CultureInfo.InvariantCulture));
        }
    }
}
