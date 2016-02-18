// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Helper;
    using Kafka.Client.Requests;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class OffsetHelper
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(OffsetHelper));

        internal static void GetAdjustedOffset<TKey, TData>(string topic, KafkaSimpleManager<TKey, TData> kafkaSimpleManager,
            int partitionID,
            KafkaOffsetType offsetType,
            long offset,
            int lastMessageCount, out long earliest, out long latest, out long offsetBase)
        {
            StringBuilder sbSummaryOnfOnePartition = new StringBuilder();

            kafkaSimpleManager.RefreshAndGetOffset(0, string.Empty, 0, topic, partitionID, true, out earliest, out latest);
            sbSummaryOnfOnePartition.AppendFormat("\t\tearliest:{0}\tlatest:{1}\tlength:{2}"
                , earliest
                , latest
                , (latest - earliest) == 0 ? "(empty)" : (latest - earliest).ToString());

            if (offsetType == KafkaOffsetType.Timestamp)
            {
                DateTime timestampVal = KafkaClientHelperUtils.DateTimeFromUnixTimestampMillis(offset);

                long timestampLong = KafkaClientHelperUtils.ToUnixTimestampMillis(timestampVal);
                try
                {
                    long timeStampOffset = kafkaSimpleManager.RefreshAndGetOffsetByTimeStamp(0, string.Empty, 0, topic, partitionID, timestampVal);

                    sbSummaryOnfOnePartition.AppendFormat("\r\n");
                    sbSummaryOnfOnePartition.AppendFormat("\t\ttimeStampOffset:{0}\ttimestamp(UTC):{1}\tTime(Local):{2}\tUnixTimestamp:{3}\t"
                    , timeStampOffset
                    , KafkaClientHelperUtils.DateTimeFromUnixTimestampMillis(timestampLong).ToString("s")
                    , timestampVal.ToString("s")
                    , timestampLong);

                    offsetBase = KafkaClientHelperUtils.GetValidStartReadOffset(offsetType, earliest, latest, timeStampOffset, lastMessageCount);
                }
                catch (TimeStampTooSmallException e)
                {
                    sbSummaryOnfOnePartition.AppendFormat("\r\n");
                    sbSummaryOnfOnePartition.AppendFormat("\t\ttimeStampOffset:{0}\ttimestamp(UTC):{1}\tTime(Local):{2}\tUnixTimestamp:{3}\t"
                    , "NA since no data before the time you specified, please retry with a bigger value."
                    , KafkaClientHelperUtils.DateTimeFromUnixTimestampMillis(timestampLong).ToString("s")
                    , timestampVal.ToString("s")
                    , timestampLong);

                    throw new ApplicationException(sbSummaryOnfOnePartition.ToString(), e);
                }
            }
            else
            {
                offsetBase = KafkaClientHelperUtils.GetValidStartReadOffset(offsetType, earliest, latest, 0, lastMessageCount);
            }

            Logger.Info(sbSummaryOnfOnePartition.ToString());
        }
    }
}
