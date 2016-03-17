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
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Single thread to produce to demo the basic process of produce data.
    /// When leader of some partition changed, will catch exception and recreate producer for all partitions.
    /// For better efficient example, check ProducePerfTestHelper.cs.
    /// </summary>
    internal class ProduceSimpleHelper
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ProduceSimpleHelper));

        private const string ClientID = "ProduceSimpleHelper";
        private static int ProducerRequestId;
        private static List<ProducerData<byte[], Message>> listOfDataNeedSendInOneBatch = new List<ProducerData<byte[], Message>>();
        private static List<byte[]> listOfKeys = new List<byte[]>();
        private static Random rand = new Random();
        private static Stopwatch stopWatch = new Stopwatch();
        private static TimeSpan ts = new TimeSpan();
        private static int totalPartitionCount = 0;
        private static DateTime lastTimeRefreshMetadata = DateTime.UtcNow;
        private static KafkaSimpleManagerConfiguration kafkaSimpleManagerConfig;
        private static ProducerConfiguration producerConfigTemplate;
        private static Dictionary<int, int> produceMessagePerPartition = new Dictionary<int, int>();
        private static Dictionary<int, int> produceMessagePerPartitionExpect = new Dictionary<int, int>();
        private static int sentBatchCount = 0;
        private static long failedMessageCount = 0;
        private static long successMessageCount = 0;

        internal static void Run(ProduceSimpleHelperOption produceOptions)
        {
            failedMessageCount = 0;
            successMessageCount = 0;
            sentBatchCount = 0;
            produceMessagePerPartition = new Dictionary<int, int>();
            produceMessagePerPartitionExpect = new Dictionary<int, int>();

            PrepareSentMessages(produceOptions);

            kafkaSimpleManagerConfig = new KafkaSimpleManagerConfiguration()
            {
                Zookeeper = produceOptions.Zookeeper,
                MaxMessageSize = SyncProducerConfiguration.DefaultMaxMessageSize,
                PartitionerClass = produceOptions.PartitionerClass
            };
            kafkaSimpleManagerConfig.Verify();
            producerConfigTemplate = new ProducerConfiguration(
                      new List<BrokerConfiguration>() { }) //The Brokers will be replaced inside of KafkaSimpleManager
            {
                ForceToPartition = -1,
                PartitionerClass = kafkaSimpleManagerConfig.PartitionerClass,
                TotalNumPartitions = 0,
                RequiredAcks = produceOptions.RequiredAcks,
                AckTimeout = produceOptions.AckTimeout,
                SendTimeout = produceOptions.SendTimeout,
                ReceiveTimeout = produceOptions.ReceiveTimeout,
                CompressionCodec = KafkaNetLibraryExample.ConvertToCodec(produceOptions.Compression.ToString()),
                BufferSize = produceOptions.BufferSize,
                SyncProducerOfOneBroker = produceOptions.SyncProducerOfOneBroker, //Actually it's sync producer socket count of one partition
                MaxMessageSize = Math.Max(SyncProducerConfiguration.DefaultMaxMessageSize, produceOptions.MessageSize)
            };

            using (ZooKeeperClient zkClient = new ZooKeeperClient(produceOptions.Zookeeper,
                        ZooKeeperConfiguration.DefaultSessionTimeout, ZooKeeperStringSerializer.Serializer))
            {
                zkClient.Connect();
                Dictionary<int, int[]> topicDataInZookeeper = ZkUtils.GetTopicMetadataInzookeeper(zkClient, produceOptions.Topic);

                // -2  by default or customized partitioner class. (you need specify PartitionerClass by -l) 
                if (produceOptions.PartitionId == -2)
                {
                    if (string.IsNullOrEmpty(kafkaSimpleManagerConfig.PartitionerClass))
                        throw new ArgumentException("The partitioer class must not be empty if you want to  send to partition by partitioner.");
                    //if (producerConfigTemplate.TotalNumPartitions <= 0)
                    //    throw new ArgumentException(string.Format("Please also specify the TotalNumPartitions  if you want to  send to partition by partitioner."));
                    ProduceByPartitionerClass(produceOptions, topicDataInZookeeper.Count);
                }
                else
                {
                    ProduceToRandomOrSpecificPartition(produceOptions);
                }
            }
        }

        private static void ProduceByPartitionerClass(ProduceSimpleHelperOption produceOptions, int partitionCountInZookeeper)
        {
            using (KafkaSimpleManager<byte[], Message> kafkaSimpleManager = new KafkaSimpleManager<byte[], Message>(kafkaSimpleManagerConfig))
            {

                Stopwatch stopwatch2 = new Stopwatch();
                stopwatch2.Start();
                int nProducers = kafkaSimpleManager.InitializeProducerPoolForTopic(KafkaNETExampleConstants.DefaultVersionId, ClientID, ProducerRequestId++, produceOptions.Topic, true, producerConfigTemplate, false);
                stopwatch2.Stop();
                Console.WriteLine("Spent {0:0.00} ms to create {1} producers. average {2:0.00} ms per producer. syncProducerOfOnePartition:{3} ", stopwatch2.Elapsed.TotalMilliseconds, nProducers, stopwatch2.Elapsed.TotalMilliseconds / nProducers, producerConfigTemplate.SyncProducerOfOneBroker);

                while (true)
                {

                    Producer<byte[], Message> producer = null;
                    try
                    {
                        producer = kafkaSimpleManager.GetProducerWithPartionerClass(produceOptions.Topic);
                        //Here the 
                        totalPartitionCount = kafkaSimpleManager.GetTopicMetadta(produceOptions.Topic).PartitionsMetadata.Count();
                        Logger.InfoFormat("Get GetProducerWithPartionerClass Producer:{0}  totalPartitionCount has metadata:{1} partitionCountInZookeeper:{2} "
                            , producer == null ? "null" : producer.ToString(), totalPartitionCount, partitionCountInZookeeper);

                        stopWatch.Start();
                        CountExpectCount(listOfKeys, produceOptions.PartitionerClass, partitionCountInZookeeper);
                        producer.Send(listOfDataNeedSendInOneBatch);
                        successMessageCount += produceOptions.MessageCountPerBatch;

                        stopWatch.Stop();
                        sentBatchCount++;

                        if (sentBatchCount % 10 == 0)
                            RegularStatistics(produceOptions, sentBatchCount);
                        if (produceOptions.BatchCount > 0 && sentBatchCount >= produceOptions.BatchCount)
                            break;

                    }
                    catch (FailedToSendMessageException<byte[]> e)
                    {
                        //Sometime the sent maybe partially success.
                        //For example, 100 message,  averagely to 5 partitions, if one partitoin has no leader.
                        // here will get the failed 20 message, you can retry or do something for them.
                        Logger.Error("===FAILED=========");
                        Logger.ErrorFormat("{0}", e.Message);
                        if (e.ProduceDispatchSeralizeResult != null
                            && e.ProduceDispatchSeralizeResult.FailedProducerDatas != null)
                        {
                            Logger.ErrorFormat("Failed produce message key: ");
                            foreach (ProducerData<byte[], Message> a in e.ProduceDispatchSeralizeResult.FailedProducerDatas)
                            {
                                Logger.ErrorFormat("Key:{0} ", System.Text.Encoding.Default.GetString(a.Key));
                            }
                        }
                        //Here partially success we also consider 
                        failedMessageCount += e.CountFailed;
                        successMessageCount += e.CountAll - e.CountFailed;
                        sentBatchCount++;
                        if (sentBatchCount % 10 == 0)
                            RegularStatistics(produceOptions, sentBatchCount);
                        if (produceOptions.BatchCount > 0 && sentBatchCount >= produceOptions.BatchCount)
                            break;
                        Logger.Error("  \r\n");
                    }
                    catch (Exception e)
                    {
                        Logger.ErrorFormat("Got exception, maybe leader change for some partition, will refresh metadata and recreate producer {0}", e.FormatException());
                        TopicMetadata topicMetadata = kafkaSimpleManager.RefreshMetadata(KafkaNETExampleConstants.DefaultVersionId, ClientID, ProducerRequestId++, produceOptions.Topic, true);
                        totalPartitionCount = topicMetadata.PartitionsMetadata.Count();
                        Logger.InfoFormat("Get GetProducerWithPartionerClass Producer:{0}  totalPartitionCount has metadata:{1} partitionCountInZookeeper:{2} "
                           , producer == null ? "null" : producer.ToString(), totalPartitionCount, partitionCountInZookeeper);
                    }

                    if (produceOptions.BatchCount > 0 && sentBatchCount >= produceOptions.BatchCount)
                        break;
                }
            }

            RegularStatistics(produceOptions, sentBatchCount);
        }

        private static void CountExpectCount(List<byte[]> listOfKeys, string p, int partitionCount)
        {
            if (p == ProducerConfiguration.DefaultPartitioner)
            {
                foreach (byte[] k in listOfKeys)
                {
                    int partitoin = Math.Abs(Encoding.UTF8.GetString(k).GetHashCode()) % partitionCount;
                    if (!produceMessagePerPartitionExpect.ContainsKey(partitoin))
                        produceMessagePerPartitionExpect.Add(partitoin, 0);
                    produceMessagePerPartitionExpect[partitoin] = produceMessagePerPartitionExpect[partitoin] + 1;
                }
            }
            else if (p == "KafkaNET.Library.Examples.CustomePartitionerSample`1")
            {
                foreach (byte[] k in listOfKeys)
                {
                    int partitoin = Math.Abs(2 * Encoding.UTF8.GetString(k).GetHashCode() / 3) % partitionCount;
                    if (!produceMessagePerPartitionExpect.ContainsKey(partitoin))
                        produceMessagePerPartitionExpect.Add(partitoin, 0);
                    produceMessagePerPartitionExpect[partitoin] = produceMessagePerPartitionExpect[partitoin] + 1;
                }
            }
            else
                throw new NotImplementedException();
        }

        private static void ProduceToRandomOrSpecificPartition(ProduceSimpleHelperOption produceOptions)
        {
            using (KafkaSimpleManager<byte[], Message> kafkaSimpleManager = new KafkaSimpleManager<byte[], Message>(kafkaSimpleManagerConfig))
            {

                Stopwatch stopwatch2 = new Stopwatch();
                stopwatch2.Start();
                int nProducers = kafkaSimpleManager.InitializeProducerPoolForTopic(KafkaNETExampleConstants.DefaultVersionId, ClientID, ProducerRequestId++, produceOptions.Topic, true, producerConfigTemplate, false);
                stopwatch2.Stop();
                Console.WriteLine("Spent {0:0.00} ms to create {1} producers. average {2:0.00} ms per producer. syncProducerOfOnePartition:{3} ", stopwatch2.Elapsed.TotalMilliseconds, nProducers, stopwatch2.Elapsed.TotalMilliseconds / nProducers, producerConfigTemplate.SyncProducerOfOneBroker);
                totalPartitionCount = kafkaSimpleManager.GetTopicMetadta(produceOptions.Topic).PartitionsMetadata.Count();

                int targetPartitionID = Int16.MinValue;
                while (true)
                {
                    Producer<byte[], Message> producer = null;
                    try
                    {
                        targetPartitionID = produceOptions.PartitionId >= 0 ? produceOptions.PartitionId : rand.Next(totalPartitionCount);
                        Logger.InfoFormat("Will try get producer for partition:{0} ", targetPartitionID);
                        producer = kafkaSimpleManager.GetProducerOfPartition(produceOptions.Topic, targetPartitionID, false);
                        Logger.InfoFormat("Get producer for  partition:{0}  Producer:{1} ", targetPartitionID, producer == null ? "null" : producer.ToString());
                        stopWatch.Start();
                        producer.Send(listOfDataNeedSendInOneBatch);
                        successMessageCount += produceOptions.MessageCountPerBatch;
                        if (!produceMessagePerPartition.ContainsKey(targetPartitionID))
                        {
                            produceMessagePerPartition.Add(targetPartitionID, 0);
                        }

                        produceMessagePerPartition[targetPartitionID] = produceMessagePerPartition[targetPartitionID] + listOfDataNeedSendInOneBatch.Count;
                        stopWatch.Stop();
                        sentBatchCount++;

                        if (sentBatchCount % 10 == 0)
                        {
                            RegularStatistics(produceOptions, sentBatchCount);
                        }

                        if (produceOptions.BatchCount > 0 && sentBatchCount >= produceOptions.BatchCount)
                        {
                            break;
                        }

                        //Maybe partition increased, need regular check if partition number has changed.
                        if (DateTime.UtcNow > lastTimeRefreshMetadata.AddMinutes(10))
                        {
                            List<string> partitoins = kafkaSimpleManager.GetTopicPartitionsFromZK(produceOptions.Topic);
                            if (partitoins.Count != targetPartitionID)
                            {
                                nProducers = kafkaSimpleManager.InitializeProducerPoolForTopic(KafkaNETExampleConstants.DefaultVersionId, ClientID, ProducerRequestId++, produceOptions.Topic, true, producerConfigTemplate, false);
                                totalPartitionCount = kafkaSimpleManager.GetTopicMetadta(produceOptions.Topic).PartitionsMetadata.Count();
                                targetPartitionID = produceOptions.PartitionId >= 0 ? produceOptions.PartitionId : rand.Next(totalPartitionCount);
                                Logger.InfoFormat("Will try get producer for partition:{0} ", targetPartitionID);
                                producer = kafkaSimpleManager.GetProducerOfPartition(produceOptions.Topic, targetPartitionID, false);
                                Logger.InfoFormat("Get producer for  partition:{0}  Producer:{1} ", targetPartitionID, producer == null ? "null" : producer.ToString());
                            }
                            lastTimeRefreshMetadata = DateTime.UtcNow;
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.ErrorFormat("Got exception, maybe leader change or not available for some partition, will refresh metadata and recreate producer {0}", e.FormatException());
                        try
                        {
                            producer = kafkaSimpleManager.RefreshMetadataAndRecreateProducerOfOnePartition(KafkaNETExampleConstants.DefaultVersionId, ClientID, ProducerRequestId++, produceOptions.Topic, targetPartitionID, true, true, producerConfigTemplate, false);
                        }
                        catch (Exception ex)
                        {
                            Logger.ErrorFormat("Got exception while RefreshMetadataAndRecreateProducerOfOnePartition, maybe leader change for some partition, will refresh metadata and recreate producer {0} after 3 seconds ...", ex.FormatException());
                        }

                        totalPartitionCount = kafkaSimpleManager.GetTopicMetadta(produceOptions.Topic).PartitionsMetadata.Count();
                        if (targetPartitionID >= totalPartitionCount)
                        {
                            targetPartitionID = produceOptions.PartitionId >= 0 ? produceOptions.PartitionId : rand.Next(totalPartitionCount);
                            Logger.InfoFormat("Will try get producer for partition:{0} ", targetPartitionID);
                            producer = kafkaSimpleManager.GetProducerOfPartition(produceOptions.Topic, targetPartitionID, false);
                            Logger.InfoFormat("Get producer for  partition:{0}  Producer:{1} ", targetPartitionID, producer == null ? "null" : producer.ToString());
                        }
                    }

                    if (produceOptions.BatchCount > 0 && sentBatchCount >= produceOptions.BatchCount)
                    {
                        break;
                    }
                }
            }

            RegularStatistics(produceOptions, sentBatchCount);
        }

        private static void RegularStatistics(ProduceSimpleHelperOption produceroundrobinOptions, int sentBatchCount)
        {
            ts = stopWatch.Elapsed;
            double mbTransferred = successMessageCount * (produceroundrobinOptions.MessageSize / 1048576.0);
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            Console.WriteLine("Run: {0} seconds, {1:0.00} MB, {2:0.00} MB/s, {3:0.00} Success message/s \r\nRoudTrip for batch: {4:0.00}ms  \r\nRoundtrip for message: {5:0.00}ms",
                ts.TotalSeconds,
                mbTransferred,
                mbTransferred / ts.TotalSeconds,
               ((double)successMessageCount) / ts.TotalSeconds,
                ts.TotalMilliseconds / sentBatchCount,
                ts.TotalMilliseconds / (successMessageCount));
            Console.WriteLine("Success message count:{0}  Failed message count: {1}", successMessageCount, failedMessageCount);
            if (produceroundrobinOptions.PartitionId == -2)
            {
                foreach (var k in produceMessagePerPartitionExpect.Keys.OrderBy(r => r))
                    Console.WriteLine("Partition:{0} Expect Sent Message:{1}  ", k, produceMessagePerPartitionExpect[k]);
            }
            else
            {
                foreach (var k in produceMessagePerPartition.Keys.OrderBy(r => r))
                    Console.WriteLine("Partition:{0} Sent Message:{1}  ", k, produceMessagePerPartition[k]);
            }
        }

        private static void PrepareSentMessages(ProduceSimpleHelperOption produceroundrobinOptions)
        {
            Console.WriteLine("start perf test, {0} ", DateTime.Now);
            listOfDataNeedSendInOneBatch = new List<ProducerData<byte[], Message>>();
            for (int i = 0; i < produceroundrobinOptions.MessageCountPerBatch; i++)
            {
                String val = KafkaClientHelperUtils.GetRandomString(produceroundrobinOptions.MessageSize);
                byte[] bVal = System.Text.Encoding.UTF8.GetBytes(val);
                byte[] bKey = System.Text.Encoding.UTF8.GetBytes(string.Format("{0:000000}", i));
                if (produceroundrobinOptions.ConstantMessageKey)
                {
                    bKey = System.Text.Encoding.UTF8.GetBytes(string.Format("{0:000000}", 0));
                }

                Message message = new Message(bVal, bKey, produceroundrobinOptions.CompressionCodec);
                listOfKeys.Add(bKey);
                listOfDataNeedSendInOneBatch.Add(new ProducerData<byte[], Message>(produceroundrobinOptions.Topic, bKey, message));
            }
        }
    }
    public class CustomePartitionerSample<TKey> : IPartitioner<TKey>
    {
        private static readonly Random Randomizer = new Random();

        public int Partition(TKey key, int numPartitions)
        {
            if (key.GetType() == typeof(byte[]))
            {
                return key == null
                ? Randomizer.Next(numPartitions)
                : Math.Abs(Encoding.UTF8.GetString((byte[])Convert.ChangeType(key, typeof(byte[]))).GetHashCode()) % numPartitions;
            }

            return key == null
                ? Randomizer.Next(numPartitions)
                : Math.Abs(2 * key.GetHashCode() / 3) % numPartitions;
        }
    }
}
