// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Helper;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class TestHelper
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(TestHelper));

        private const int TestBug1490652MessageCountPerPartition = 6;
        private static int PartitionCount = 0;
        private static Dictionary<int, Dictionary<int, string>> TestBug1490652DataSent = new Dictionary<int, Dictionary<int, string>>();
        private static Dictionary<int, Dictionary<int, string>> TestBug1490652DataRead = new Dictionary<int, Dictionary<int, string>>();

        /// <summary>
        /// Run one test case
        /// </summary>
        /// <param name="testOptions"></param>
        public static void Run(TestHelperOptions testOptions)
        {
            bool testResult = true;
            if (testOptions.Case.ToLowerInvariant() == "bug1490652")
                testResult &= TestBug1490652(testOptions);
            if (testResult)
                Console.WriteLine("PASS");
            else
            {
                Logger.Error("Test cases FAILED!");
            }
        }

        /// <summary>
        /// Previously , the wrong exception hit when:
        ///     One broker is leader of multiple partition(>=2).
        ///     Send a group of message, and they need go to different partitions of the same broker.
        /// Preparation before run the case:
        ///     Assume we have 3 broker, then create topic as
        ///         .\bin\kafka.cmd topiccmd --create --topic mvlogsA --partition 4 --replication-factor 1 --zookeeper localhost
        ///     Then run this case by send more than 5 messages.
        /// </summary>
        /// <param name="testOptions"></param>
        /// <returns></returns>
        private static bool TestBug1490652(TestHelperOptions testOptions)
        {
            try
            {
                //Send data
                TestBug1490652SendData(testOptions);
                DumpDictToFile(TestBug1490652DataSent, "Sent.log");

                //Read data
                TestBug1490652ReadData(testOptions);
                DumpDictToFile(TestBug1490652DataRead, "Read.log");
                //Verify
                return TestBug1490652Verify(testOptions);
            }
            catch (Exception e)
            {
                Logger.ErrorFormat("Dump topic got exception:{0}\r\ninput parameter: {1} \r\n"
                     , e.FormatException(), testOptions.ToString());
                return false;
            }
        }

        private static void TestBug1490652SendData(TestHelperOptions testOptions)
        {
            int correlationID = 0;
            Random rand = new Random();
            StringBuilder sb = new StringBuilder();
            try
            {
                KafkaSimpleManagerConfiguration config = new KafkaSimpleManagerConfiguration()
                {
                    Zookeeper = testOptions.Zookeeper,
                    MaxMessageSize = SyncProducerConfiguration.DefaultMaxMessageSize
                };
                config.Verify();
                using (KafkaSimpleManager<int, Kafka.Client.Messages.Message> kafkaSimpleManager = new KafkaSimpleManager<int, Kafka.Client.Messages.Message>(config))
                {
                    TopicMetadata topicMetadata = kafkaSimpleManager.RefreshMetadata(0, "ClientID", correlationID++, testOptions.Topic, true);
                    PartitionCount = topicMetadata.PartitionsMetadata.Count();
                    List<ProducerData<int, Message>> listOfDataNeedSendInOneBatch = new List<ProducerData<int, Message>>();
                    for (int i = 0; i < PartitionCount; i++)
                    {
                        TestBug1490652DataSent.Add(i, new Dictionary<int, string>());
                        for (int j = 0; j < TestBug1490652MessageCountPerPartition; j++)
                        {
                            string val = KafkaClientHelperUtils.GetRandomString(testOptions.MessageSize);
                            byte[] bVal = System.Text.Encoding.UTF8.GetBytes(val);
                            //Set the key to partitionID, so it can directly fall into  that partition.
                            Message message = new Message(bVal, CompressionCodecs.DefaultCompressionCodec);
                            listOfDataNeedSendInOneBatch.Add(new ProducerData<int, Message>(testOptions.Topic, i, message));
                            TestBug1490652DataSent[i].Add(j, val);
                        }
                    }

                    ProducerConfiguration producerConfig = new ProducerConfiguration(new List<BrokerConfiguration>() { })
                    {
                        PartitionerClass = ProducerConfiguration.DefaultPartitioner,
                        RequiredAcks = 1,
                        BufferSize = config.BufferSize,
                        ZooKeeper = config.ZookeeperConfig,
                        MaxMessageSize = Math.Max(config.MaxMessageSize, Math.Max(SyncProducerConfiguration.DefaultMaxMessageSize, testOptions.MessageSize))
                    };
                    producerConfig.SyncProducerOfOneBroker = 1;
                    Producer<int, Kafka.Client.Messages.Message> producer = new Producer<int, Kafka.Client.Messages.Message>(producerConfig);
                    producer.Send(listOfDataNeedSendInOneBatch);
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Produce data Got exception:{0}\r\ninput parameter: {1}\r\n"
                     , ex.FormatException(), testOptions.ToString());
            }
        }

        private static void TestBug1490652ReadData(TestHelperOptions testOptions)
        {
            KafkaSimpleManagerConfiguration config = new KafkaSimpleManagerConfiguration()
            {
                FetchSize = KafkaSimpleManagerConfiguration.DefaultFetchSize,
                BufferSize = KafkaSimpleManagerConfiguration.DefaultBufferSize,
                MaxWaitTime = 0,
                MinWaitBytes = 0,
                Zookeeper = testOptions.Zookeeper
            };
            config.Verify();

            using (KafkaSimpleManager<int, Message> kafkaSimpleManager = new KafkaSimpleManager<int, Message>(config))
            {
                TopicMetadata topicMetadata = kafkaSimpleManager.RefreshMetadata(0, "ClientID", 0, testOptions.Topic, true);
                PartitionCount = topicMetadata.PartitionsMetadata.Count();
                for (int i = 0; i < PartitionCount; i++)
                {
                    #region Get real offset and adjust
                    long earliest = 0;
                    long latest = 0;
                    long offsetBase = 0;
                    OffsetHelper.GetAdjustedOffset<int, Message>(testOptions.Topic, kafkaSimpleManager, i, KafkaOffsetType.Earliest, 0,
                     0, out earliest, out latest, out offsetBase);
                    #endregion

                    TestBug1490652DataRead.Add(i, ConsumeDataOfOnePartitionTotally<int, Message>(testOptions.Topic, kafkaSimpleManager, i, KafkaOffsetType.Earliest,
                        0, 0, latest, 0, 100, -1, "DumpLog.log"));
                }
            }
        }

        internal static Dictionary<int, string> ConsumeDataOfOnePartitionTotally<TKey, TData>(string topic, KafkaSimpleManager<TKey, TData> kafkaSimpleManager,
          int partitionID,
          KafkaOffsetType offsetType,
          long offsetBase,
          long earliest,
          long latest,
          int lastMessageCount,
          int wait,
          long count,
          string file
          )
        {
            int totalCount = 0;
            int correlationID = 0;
            Dictionary<int, string> dict = new Dictionary<int, string>();

            Random rand = new Random();
            StringBuilder sb = new StringBuilder();

            using (FileStream fs = File.Open(file, FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                using (StreamWriter sw = new StreamWriter(fs))
                {
                    #region repeatly consume and dump data
                    long offsetLast = -1;
                    long l = 0;
                    sw.WriteLine("Will read partition {0} from {1}.   Earliese:{2} Last:{3} ", partitionID, offsetBase, earliest, latest);
                    Logger.InfoFormat("Will read partition {0} from {1}.   Earliese:{2} Last:{3} ", partitionID, offsetBase, earliest, latest);
                    using (Consumer consumer = kafkaSimpleManager.GetConsumer(topic, partitionID))
                    {
                        while (true)
                        {
                            correlationID++;

                            List<MessageAndOffset> listMessageAndOffsets = new List<MessageAndOffset>();

                            if (listMessageAndOffsets == null)
                            {
                                Logger.Error("PullMessage got null  List<MessageAndOffset>, please check log for detail.");
                                break;
                            }
                            else
                            {
                                offsetLast = listMessageAndOffsets.Last().MessageOffset;
                                #region dump response.Payload
                                foreach (var a in listMessageAndOffsets)
                                {
                                    dict.Add((int)a.MessageOffset, Encoding.UTF8.GetString(a.Message.Payload));
                                }

                                if (listMessageAndOffsets.Any())
                                {
                                    totalCount += listMessageAndOffsets.Count;
                                    sw.WriteLine("Finish read partition {0} to {1}.   Earliese:{2} Last:{3} ", partitionID, offsetLast, earliest, latest);
                                    Logger.InfoFormat("Finish read partition {0} to {1}.   Earliese:{2} Last:{3} ", partitionID, offsetLast, earliest, latest);
                                    offsetBase = offsetLast + 1;
                                }
                                else
                                {
                                    if (offsetBase == latest)
                                        sw.WriteLine("Hit end of queue.");
                                    sw.WriteLine("Finish read partition {0} to {1}.   Earliese:{2} Last:{3} ", partitionID, offsetLast, earliest, latest);
                                    Logger.InfoFormat("Finish read partition {0} to {1}.   Earliese:{2} Last:{3} ", partitionID, offsetLast, earliest, latest);
                                    break;
                                }
                                Thread.Sleep(wait);
                                #endregion
                            }
                            l++;
                            if (offsetBase == latest)
                                break;
                        }
                    }
                    #endregion
                    Logger.InfoFormat("Topic:{0} Partitoin:{1} Finish Read.    Earliest:{2} Latest:{3},  totalCount:{4} "
                                   , topic, partitionID, earliest, latest, totalCount);
                    sw.WriteLine("Topic:{0} Partitoin:{1} Finish Read.    Earliest:{2} Latest:{3},  totalCount:{4} \r\n "
                                   , topic, partitionID, earliest, latest, totalCount);
                }
            }
            return dict;
        }


        private static void DumpDictToFile(Dictionary<int, Dictionary<int, string>> dict, string f)
        {
            var result = dict.Select(r => new { r.Key, r.Value }).OrderBy(r => r.Key).ToList();
            using (StreamWriter sw = new StreamWriter(f, false))
            {
                foreach (var r in result)
                {
                    sw.WriteLine("PartitionID: {0}", r.Key);
                    sw.WriteLine("\tValueCount: {0}", r.Value.Count);
                    var resultOfOnePartition = r.Value.Select(a => new { a.Key, a.Value }).OrderBy(a => a.Key).ToList();
                    int idx = 0;
                    foreach (var rd in resultOfOnePartition)
                    {
                        sw.WriteLine("\tKey: {0}  Sequence/Offset:{1}  ValueLength:{2}", rd.Key, idx, rd.Value.Length);
                        sw.WriteLine("\t\t===={0}====", rd.Value);
                        idx++;
                    }
                    sw.WriteLine("");
                    sw.WriteLine("");
                }
            }
        }
        private static bool TestBug1490652Verify(TestHelperOptions testOptions)
        {

            bool verifyResult = true;
            foreach (KeyValuePair<int, Dictionary<int, string>> kv in TestBug1490652DataSent)
            {
                if (!TestBug1490652DataRead.ContainsKey(kv.Key))
                {
                    verifyResult = false;
                    Logger.ErrorFormat("Result doesn't contains data for partition {0} ", kv.Key);
                }
                else
                {
                    Dictionary<int, string> expectedResultOfOnePartition = kv.Value;
                    Dictionary<int, string> resultOfOnePartition = TestBug1490652DataRead[kv.Key];
                    foreach (KeyValuePair<int, string> kv2 in expectedResultOfOnePartition)
                    {
                        if (resultOfOnePartition[kv2.Key] != kv2.Value)
                        {
                            verifyResult = false;
                            Logger.ErrorFormat("Result doesn't match for partition  {0}  offset {1} data {2} vs {3} ", kv.Key, kv2.Key, kv2.Value, resultOfOnePartition[kv2.Key]);
                        }
                    }
                }
            }
            return verifyResult;
        }
    }
}
