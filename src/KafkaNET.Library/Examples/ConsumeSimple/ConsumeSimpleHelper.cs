// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Helper;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Requests;
    using Kafka.Client.Responses;
    using Kafka.Client.Utils;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class ConsumeSimpleHelper
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumeSimpleHelper));

        private const string ClientID = "KafkaNETLibConsoleConsumer";
        private const string DumpDataError = "Got ERROR while consume data, please check log file.";
        private static int correlationID = 0;
        private static int totalCountUTF8 = 0;
        private static int totalCountOriginal = 0;
        private static int lastNotifytotalCount = 0;
        private static int totalCount = 0;

        internal static void ConsumeDataSimple(ConsumeDataHelperArguments dumpdataOptions)
        {
            correlationID = 0;
            totalCountUTF8 = 0;
            totalCountOriginal = 0;
            totalCount = 0;
            lastNotifytotalCount = 0;
            KafkaSimpleManagerConfiguration config = new KafkaSimpleManagerConfiguration()
            {
                FetchSize = dumpdataOptions.FetchSize,
                BufferSize = dumpdataOptions.BufferSize,
                MaxWaitTime = dumpdataOptions.MaxWaitTime,
                MinWaitBytes = dumpdataOptions.MinWaitBytes,
                Zookeeper = dumpdataOptions.Zookeeper
            };
            config.Verify();

            bool finish = false;
            try
            {
                using (KafkaSimpleManager<int, Message> kafkaSimpleManager = new KafkaSimpleManager<int, Message>(config))
                {
                    TopicMetadata topicMetadata = kafkaSimpleManager.RefreshMetadata(0, ClientID, correlationID++, dumpdataOptions.Topic, true);
                    while (true)
                    {
                        try
                        {
                            for (int i = 0; i <= topicMetadata.PartitionsMetadata.Max(r => r.PartitionId); i++)
                            {
                                if (dumpdataOptions.PartitionIndex == -1 || i == dumpdataOptions.PartitionIndex)
                                {
                                    #region Get real offset and adjust
                                    long earliest = 0;
                                    long latest = 0;
                                    long offsetBase = 0;
                                    OffsetHelper.GetAdjustedOffset<int, Message>(dumpdataOptions.Topic
                                        , kafkaSimpleManager, i
                                        , KafkaNetLibraryExample.ConvertOffsetType(dumpdataOptions.Offset)
                                        , KafkaNetLibraryExample.ConvertOffset(dumpdataOptions.Offset)
                                        , dumpdataOptions.LastMessagesCount, out earliest, out latest, out offsetBase);
                                    #endregion
                                    Console.WriteLine("Topic:{0} Partition:{1} will read from {2} earliest:{3} latest:{4}", dumpdataOptions.Topic, i, offsetBase, earliest, latest);
                                    finish = ConsumeDataOfOnePartition(kafkaSimpleManager, i, offsetBase, earliest, latest, dumpdataOptions);
                                    if (finish)
                                        break;
                                }
                            }

                            finish = true;
                        }
                        catch (Exception ex)
                        {
                            Logger.ErrorFormat("ConsumeDataSimple Got exception, will refresh metadata. {0}", ex.FormatException());
                            kafkaSimpleManager.RefreshMetadata(0, ClientID, correlationID++, dumpdataOptions.Topic, true);
                        }

                        if (finish)
                            break;
                    }
                }

                Logger.InfoFormat("Topic:{0} Finish Read.     totalCount:{1} ", dumpdataOptions.Topic, totalCount);
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("ConsumeDataSimple  Got exception:{0}\r\ninput parameter: {1}", ex.FormatException(), dumpdataOptions.ToString());
            }
        }

        private static bool ConsumeDataOfOnePartition<TKey, TData>(KafkaSimpleManager<TKey, TData> kafkaSimpleManager,
            int partitionID,
            long offsetBase, long earliest, long latest,
            ConsumeDataHelperArguments dumpdataOptions)
        {
            Random rand = new Random();
            StringBuilder sb = new StringBuilder();
            using (FileStream fs = File.Open(dumpdataOptions.File, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                using (StreamWriter sw = new StreamWriter(fs))
                {
                    #region repeatly consume and dump data
                    long offsetLast = -1;
                    long l = 0;
                    sw.WriteLine("Will read partition {0} from {1}.   Earliese:{2} latest:{3} ", partitionID, offsetBase, earliest, latest);
                    Logger.InfoFormat("Will read partition {0} from {1}.   Earliese:{2} latest:{3} ", partitionID, offsetBase, earliest, latest);
                    using (Consumer consumer = kafkaSimpleManager.GetConsumer(dumpdataOptions.Topic, partitionID))
                    {
                        while (true)
                        {
                            correlationID++;
                            List<MessageAndOffset> listMessageAndOffsets = ConsumeSimpleHelper.FetchAndGetMessageAndOffsetList(consumer
                                                        , correlationID++,
                                                        dumpdataOptions.Topic, partitionID, offsetBase,
                                                        consumer.Config.FetchSize,
                                                        kafkaSimpleManager.Config.MaxWaitTime,
                                                        kafkaSimpleManager.Config.MinWaitBytes);

                            if (listMessageAndOffsets == null)
                            {
                                Logger.Error("PullMessage got null  List<MessageAndOffset>, please check log for detail.");
                                break;
                            }
                            else
                            {

                                #region dump response.Payload
                                if (listMessageAndOffsets.Any())
                                {
                                    offsetLast = listMessageAndOffsets.Last().MessageOffset;
                                    totalCount += listMessageAndOffsets.Count;
                                    KafkaConsoleUtil.DumpDataToFile(dumpdataOptions.DumpDataAsUTF8, dumpdataOptions.DumpBinaryData, sw, fs, listMessageAndOffsets, dumpdataOptions.Count, offsetBase, ref totalCountUTF8, ref totalCountOriginal);
                                    sw.WriteLine("Finish read partition {0} to {1}.   Earliese:{2} latest:{3} ", partitionID, offsetLast, earliest, latest);
                                    Logger.InfoFormat("Finish read partition {0} to {1}.   Earliese:{2} latest:{3} ", partitionID, offsetLast, earliest, latest);
                                    offsetBase = offsetLast + 1;
                                    if (totalCount - lastNotifytotalCount > 1000)
                                    {
                                        Console.WriteLine("Partition: {0} totally read  {1}  will continue read from   {2}", partitionID, totalCount, offsetBase);
                                        lastNotifytotalCount = totalCount;
                                    }
                                }
                                else
                                {
                                    if (offsetBase == latest)
                                        sw.WriteLine("Hit end of queue.");
                                    sw.WriteLine("Finish read partition {0} to {1}.   Earliese:{2} latest:{3} ", partitionID, offsetLast, earliest, latest);
                                    Logger.InfoFormat("Finish read partition {0} to {1}.   Earliese:{2} latest:{3} ", partitionID, offsetLast, earliest, latest);
                                    Console.WriteLine("Partition: {0} totally read  {1}  Hit end of queue   {2}", partitionID, totalCount, offsetBase);
                                    break;
                                }
                                Thread.Sleep(1000);
                                #endregion
                            }
                            l++;
                            if (totalCount >= dumpdataOptions.Count && dumpdataOptions.Count > 0)
                                return true;
                        }
                    }
                    #endregion
                    Logger.InfoFormat("Topic:{0} Partitoin:{1} Finish Read.    Earliest:{2} Latest:{3},  totalCount:{4} "
                                   , dumpdataOptions.Topic, partitionID, earliest, latest, totalCount);
                    sw.WriteLine("Topic:{0} Partitoin:{1} Finish Read.    Earliest:{2} Latest:{3},  totalCount:{4} \r\n "
                                   , dumpdataOptions.Topic, partitionID, earliest, latest, totalCount);
                }
            }

            if (totalCount >= dumpdataOptions.Count && dumpdataOptions.Count > 0)
                return true;
            else
                return false;
        }

        private static List<MessageAndOffset> FetchAndGetMessageAndOffsetList(
            Consumer consumer,
            int correlationID,
            string topic,
            int partitionIndex,
            long fetchOffset,
            int fetchSize,
            int maxWaitTime,
            int minWaitSize)
        {
            List<MessageAndOffset> listMessageAndOffsets = new List<MessageAndOffset>();
            PartitionData partitionData = null;
            int payloadCount = 0;
            // at least retry once
            int maxRetry = 1;
            int retryCount = 0;
            string s = string.Empty;
            bool success = false;
            while (!success && retryCount < maxRetry)
            {
                try
                {
                    FetchResponse response = consumer.Fetch(Assembly.GetExecutingAssembly().ManifestModule.ToString(),      // client id
                        topic,
                        correlationID, //random.Next(int.MinValue, int.MaxValue),                        // correlation id
                        partitionIndex,
                        fetchOffset,
                        fetchSize,
                        maxWaitTime,
                        minWaitSize);

                    if (response == null)
                    {
                        throw new KeyNotFoundException(string.Format("FetchRequest returned null response,fetchOffset={0},leader={1},topic={2},partition={3}",
                            fetchOffset, consumer.Config.Broker, topic, partitionIndex));
                    }

                    partitionData = response.PartitionData(topic, partitionIndex);
                    if (partitionData == null)
                    {
                        throw new KeyNotFoundException(string.Format("PartitionData is null,fetchOffset={0},leader={1},topic={2},partition={3}",
                            fetchOffset, consumer.Config.Broker, topic, partitionIndex));
                    }

                    if (partitionData.Error == ErrorMapping.OffsetOutOfRangeCode)
                    {
                        s = "PullMessage OffsetOutOfRangeCode,change to Latest,topic={0},leader={1},partition={2},FetchOffset={3},retryCount={4},maxRetry={5}";
                        Logger.ErrorFormat(s, topic, consumer.Config.Broker, partitionIndex, fetchOffset, retryCount, maxRetry);
                        return null;
                    }

                    if (partitionData.Error != ErrorMapping.NoError)
                    {
                        s = "PullMessage ErrorCode={0},topic={1},leader={2},partition={3},FetchOffset={4},retryCount={5},maxRetry={6}";
                        Logger.ErrorFormat(s, partitionData.Error, topic, consumer.Config.Broker, partitionIndex, fetchOffset, retryCount, maxRetry);
                        return null;
                    }

                    success = true;
                    listMessageAndOffsets = partitionData.GetMessageAndOffsets();
                    if (null != listMessageAndOffsets && listMessageAndOffsets.Any())
                    {
                        //TODO: When key are same for sequence of message, need count payloadCount by this way.  So why line 438 work? is there bug?
                        payloadCount = listMessageAndOffsets.Count;// messages.Count();

                        long lastOffset = listMessageAndOffsets.Last().MessageOffset;

                        if ((payloadCount + fetchOffset) != (lastOffset + 1))
                        {
                            s = "PullMessage offset payloadCount out-of-sync,topic={0},leader={1},partition={2},payloadCount={3},FetchOffset={4},lastOffset={5},retryCount={6},maxRetry={7}";
                            Logger.ErrorFormat(s, topic, consumer.Config.Broker, partitionIndex, payloadCount, fetchOffset, lastOffset, retryCount, maxRetry);
                        }
                    }

                    return listMessageAndOffsets;
                }
                catch (Exception)
                {
                    if (retryCount >= maxRetry)
                    {
                        throw;
                    }
                }
                finally
                {
                    retryCount++;
                }
            } // end of while loop

            return listMessageAndOffsets;
        }
    }
}
