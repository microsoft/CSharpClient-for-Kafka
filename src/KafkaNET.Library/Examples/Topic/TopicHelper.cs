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
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration;
    using Microsoft.KafkaNET.Library.Util;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Web.Script.Serialization;
    using ZooKeeperNet;

    internal class TopicHelper
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(TopicHelper));

        private const string DumpTopicError = "Got ERROR while dump topic {0} , please check log file.";
        private const string ClientID = "KafkaNETLibConsoleTopic";
        private static int TopicMetadataRequestID = 0;

        internal static void DumpTopicMetadataAndOffset(TopicHelperArguments dtOptions)
        {
            string topics = dtOptions.Topic;
            string zookeeper = dtOptions.Zookeeper;
            int partitionIndex = dtOptions.PartitionIndex;
            bool includePartitionDetailInfo = true;
            bool includeOffsetInfo = true;
            DateTime timestamp = dtOptions.TimestampInUTC;
            bool dumpToLog = true;
            bool dumpToConsole = true;
            string file = dtOptions.File;

            using (ZooKeeperClient zkClient = new ZooKeeperClient(zookeeper,
                            ZooKeeperConfiguration.DefaultSessionTimeout, ZooKeeperStringSerializer.Serializer))
            {
                zkClient.Connect();
                //BrokerID -->Count of as leader
                SortedDictionary<int, int> parttionBrokerID_LeaderCountDistrib = new SortedDictionary<int, int>();
                //BrokerID -->Count of as replica
                SortedDictionary<int, int> parttionBrokerID_ReplicaCountDistrib = new SortedDictionary<int, int>();

                StringBuilder sbAll = new StringBuilder();
                int topicCount = 0;
                if (string.IsNullOrEmpty(topics))
                {
                    List<string> topicList = new List<string>();
                    string path = "/brokers/topics";
                    try
                    {
                        IEnumerable<string> ts = zkClient.GetChildren(path);
                        foreach (var p in ts)
                        {
                            topicList.Add(p);
                        }
                    }
                    catch (KeeperException e)
                    {
                        if (e.ErrorCode == KeeperException.Code.NONODE)
                        {
                            throw new ApplicationException("Please make sure the path exists in zookeeper:  " + path, e);
                        }
                        else
                            throw;
                    }

                    sbAll.AppendFormat("\r\nTotally {0} topics. \r\n\r\n", topicList.Count);
                    foreach (var t in topicList.ToArray().OrderBy(r => r).ToArray())
                    {
                        SortedDictionary<int, long> latestOffsetDict = new SortedDictionary<int, long>();
                        SortedDictionary<int, long> latestLength = new SortedDictionary<int, long>();
                        sbAll.Append(DumpTopicMetadataAndOffsetInternal(zkClient, t,
                        zookeeper,
                        partitionIndex,
                        includePartitionDetailInfo,
                        includeOffsetInfo,
                        timestamp,
                        parttionBrokerID_LeaderCountDistrib, parttionBrokerID_ReplicaCountDistrib, latestOffsetDict, latestLength));
                    }
                    topicCount = topicList.Count;
                }
                else if (topics.Contains(","))
                {
                    string[] topicArray = topics.Split(new char[] { ',' });
                    topicCount = topicArray.Length;
                    sbAll.AppendFormat("\r\nTotally {0} topics. \r\n\r\n", topicArray.Length);
                    foreach (var t in topicArray.OrderBy(r => r).ToArray())
                    {
                        SortedDictionary<int, long> latestOffsetDict = new SortedDictionary<int, long>();
                        SortedDictionary<int, long> latestLength = new SortedDictionary<int, long>();
                        sbAll.Append(DumpTopicMetadataAndOffsetInternal(zkClient, t,
                       zookeeper,
                       partitionIndex,
                       includePartitionDetailInfo,
                       includeOffsetInfo,
                       timestamp,
                       parttionBrokerID_LeaderCountDistrib, parttionBrokerID_ReplicaCountDistrib, latestOffsetDict, latestLength));
                    }
                }
                else
                {
                    SortedDictionary<int, long> latestOffsetDict = new SortedDictionary<int, long>();
                    SortedDictionary<int, long> latestLength = new SortedDictionary<int, long>();
                    sbAll.Append(DumpTopicMetadataAndOffsetInternal(zkClient, topics,
                        zookeeper,
                        partitionIndex,
                        includePartitionDetailInfo,
                        includeOffsetInfo,
                        timestamp,
                       parttionBrokerID_LeaderCountDistrib, parttionBrokerID_ReplicaCountDistrib, latestOffsetDict, latestLength));
                    topicCount = 1;
                }

                if (topicCount > 1)
                {
                    sbAll.AppendFormat("\r\nBroker as leader distribution=====All topic=======\r\n");
                    sbAll.AppendFormat("\r\tBrokerID\tLeadPartition count\r\n");
                    foreach (KeyValuePair<int, int> kv in parttionBrokerID_LeaderCountDistrib)
                    {
                        sbAll.AppendFormat("\t\t{0}\t{1}\r\n", KafkaConsoleUtil.GetBrokerIDAndIP(kv.Key), kv.Value);
                    }

                    sbAll.AppendFormat("Broker as replica distribution========All topic=====\r\n");
                    sbAll.AppendFormat("\r\tBrokerID\tReplication count count\r\n");
                    foreach (KeyValuePair<int, int> kv in parttionBrokerID_ReplicaCountDistrib)
                    {
                        sbAll.AppendFormat("\t\t{0}\t{1}\r\n", KafkaConsoleUtil.GetBrokerIDAndIP(kv.Key), kv.Value);
                    }
                }

                string s = sbAll.ToString();
                if (dumpToLog)
                {
                    Logger.Info(s);
                }

                if (dumpToConsole)
                {
                    Console.WriteLine(s);
                }

                if (!string.IsNullOrEmpty(file))
                {
                    Console.WriteLine("Will write to {0}", file);
                    using (StreamWriter sw = new StreamWriter(file, false))
                    {
                        sw.WriteLine(s);
                    }
                }
            }
        }

        internal static string DumpTopicMetadataAndOffsetInternal(ZooKeeperClient zkClient, string topic,
           string zookeeper,
           int partitionIndex,
           bool includePartitionDetailInfo,
           bool includeOffsetInfo,
           DateTime timestamp,
           SortedDictionary<int, int> parttionBrokerID_LeaderCountDistribAll,
           SortedDictionary<int, int> parttionBrokerID_ReplicaCountDistribAll,
           SortedDictionary<int, long> latestOffset,
           SortedDictionary<int, long> latestLength)
        {
            StringBuilder sb = new StringBuilder();
            string s = string.Empty;
            //BrokerID -->Count of as leader
            SortedDictionary<int, int> parttionBrokerID_LeaderCountDistrib = new SortedDictionary<int, int>();
            //BrokerID -->Count of as replica
            SortedDictionary<int, int> parttionBrokerID_ReplicaCountDistrib = new SortedDictionary<int, int>();
            try
            {
                if (string.IsNullOrEmpty(zookeeper))
                {
                    Logger.Error(" zookeeper  should be provided");
                    sb.AppendFormat(DumpTopicError, topic);
                }
                else
                {
                    KafkaSimpleManagerConfiguration config = new KafkaSimpleManagerConfiguration()
                    {
                        Zookeeper = zookeeper
                    };
                    config.Verify();
                    Dictionary<int, int[]> detailDataInZookeeper = ZkUtils.GetTopicMetadataInzookeeper(zkClient, topic);
                    using (KafkaSimpleManager<int, Message> kafkaSimpleManager = new KafkaSimpleManager<int, Message>(config))
                    {
                        TopicMetadata topicMetadata = kafkaSimpleManager.RefreshMetadata(KafkaNETExampleConstants.DefaultVersionId, ClientID, TopicMetadataRequestID++, topic, true);

                        int partitionCount = topicMetadata.PartitionsMetadata.Count();
                        sb.AppendFormat("Topic:{0}\tPartitionCount:{1}\t", topic, partitionCount);

                        int replicationFactor = Enumerable.Count<Broker>(topicMetadata.PartitionsMetadata.First().Replicas);
                        sb.AppendFormat("ReplicationFactor:{0}\t", replicationFactor);

                        //TODO:  compare detailDataInZookeeper and check which one missed.
                        StringBuilder sbDetail = new StringBuilder();
                        if (includePartitionDetailInfo)
                        {
                            long sumEndOffset = 0;
                            long sumLength = 0;
                            foreach (PartitionMetadata p in topicMetadata.PartitionsMetadata.OrderBy(r => r.PartitionId).ToList())
                            {
                                int[] replicaInZookeeper = null;
                                if (detailDataInZookeeper.ContainsKey(p.PartitionId))
                                {
                                    replicaInZookeeper = detailDataInZookeeper[p.PartitionId];
                                    detailDataInZookeeper.Remove(p.PartitionId);
                                }

                                #region One partition
                                long earliest = 0;
                                long latest = 0;
                                if (partitionIndex == -1 || p.PartitionId == partitionIndex)
                                {
                                    //sbDetail.AppendFormat("\tTopic:{0}", topic);
                                    sbDetail.AppendFormat("\tPartition:{0}", p.PartitionId);
                                    if (p.Leader != null)
                                    {
                                        sbDetail.AppendFormat("\tLeader:{0}", KafkaConsoleUtil.GetBrokerIDAndIP(p.Leader.Id));

                                        if (parttionBrokerID_LeaderCountDistrib.ContainsKey(p.Leader.Id))
                                            parttionBrokerID_LeaderCountDistrib[p.Leader.Id]++;
                                        else
                                            parttionBrokerID_LeaderCountDistrib.Add(p.Leader.Id, 1);

                                        if (parttionBrokerID_LeaderCountDistribAll.ContainsKey(p.Leader.Id))
                                            parttionBrokerID_LeaderCountDistribAll[p.Leader.Id]++;
                                        else
                                            parttionBrokerID_LeaderCountDistribAll.Add(p.Leader.Id, 1);
                                    }
                                    else
                                        sbDetail.AppendFormat("\tLeader:NoLeader!");

                                    sbDetail.AppendFormat("\tReplicas:{0}", string.Join(",", p.Replicas.Select(r => KafkaConsoleUtil.GetBrokerIDAndIP(r.Id)).ToArray()));
                                    foreach (Broker b in p.Replicas)
                                    {
                                        if (parttionBrokerID_ReplicaCountDistrib.ContainsKey(b.Id))
                                            parttionBrokerID_ReplicaCountDistrib[b.Id]++;
                                        else
                                            parttionBrokerID_ReplicaCountDistrib.Add(b.Id, 1);

                                        if (parttionBrokerID_ReplicaCountDistribAll.ContainsKey(b.Id))
                                            parttionBrokerID_ReplicaCountDistribAll[b.Id]++;
                                        else
                                            parttionBrokerID_ReplicaCountDistribAll.Add(b.Id, 1);
                                    }

                                    //sbDetail.AppendFormat("\tIsr:{0}", string.Join(",", p.Isr.Select(r => r.Id).ToArray()));
                                    ArrayList isrs = GetIsr(zkClient, topic, p.PartitionId);
                                    sbDetail.AppendFormat("\tIsr:{0}", string.Join(",", isrs.ToArray().Select(r => KafkaConsoleUtil.GetBrokerIDAndIP(Convert.ToInt32(r)))));
                                    //TODO: add missed replica

                                    #region Offset
                                    if (includeOffsetInfo)
                                    {
                                        try
                                        {
                                            kafkaSimpleManager.RefreshAndGetOffset(KafkaNETExampleConstants.DefaultVersionId, ClientID, TopicMetadataRequestID++, topic, p.PartitionId, true, out earliest, out latest);
                                            sumEndOffset += latest;
                                            sumLength += (latest - earliest);
                                            sbDetail.AppendFormat("\tlength:{2}\tearliest:{0}\tlatest:{1}"
                                                , earliest
                                                , latest
                                                , (latest - earliest) == 0 ? "(empty)" : (latest - earliest).ToString());
                                            sbDetail.AppendFormat("\r\n");

                                            latestOffset.Add(p.PartitionId, latest);
                                            latestLength.Add(p.PartitionId, latest - earliest);
                                        }
                                        catch (NoLeaderForPartitionException e)
                                        {
                                            sbDetail.AppendFormat(" ERROR:{0}\r\n", e.Message);
                                        }
                                        catch (UnableToConnectToHostException e)
                                        {
                                            sbDetail.AppendFormat(" ERROR:{0}\r\n", e.Message);
                                        }

                                        if (timestamp != DateTime.MinValue)
                                        {

                                            long timestampLong = KafkaClientHelperUtils.ToUnixTimestampMillis(timestamp);
                                            try
                                            {
                                                long timeStampOffset = kafkaSimpleManager.RefreshAndGetOffsetByTimeStamp(KafkaNETExampleConstants.DefaultVersionId, ClientID, TopicMetadataRequestID++, topic, p.PartitionId, timestamp);
                                                sbDetail.AppendFormat("\t\ttimeStampOffset:{0}\ttimestamp(UTC):{1}\tUnixTimestamp:{2}\t"
                                               , timeStampOffset
                                               , KafkaClientHelperUtils.DateTimeFromUnixTimestampMillis(timestampLong).ToString("s")
                                               , timestampLong);
                                                sbDetail.AppendFormat("\r\n");
                                            }
                                            catch (TimeStampTooSmallException)
                                            {
                                                sbDetail.AppendFormat("\t\ttimeStampOffset:{0}\ttimestamp(UTC):{1}\tUnixTimestamp:{2}\t"
                                                 , "NA since no data before the time you specified, please retry with a bigger value."
                                                 , KafkaClientHelperUtils.DateTimeFromUnixTimestampMillis(timestampLong).ToString("s")
                                                 , timestampLong);
                                                sbDetail.AppendFormat("\r\n");
                                            }

                                        }
                                    }
                                    #endregion
                                }
                                #endregion
                            }
                            if (includeOffsetInfo)
                            {
                                sb.AppendFormat("SumeEndOffset:{0:0,0}  SumLength:{1:0,0}\r\n", sumEndOffset, sumLength);
                            }
                            else
                            {
                                sb.AppendFormat("\r\n");
                            }

                            if (detailDataInZookeeper.Any())
                            {
                                foreach (KeyValuePair<int, int[]> kv in detailDataInZookeeper)
                                {
                                    sb.AppendFormat("=ERROR=MISSED partition= {0}  Replicas {1} ", kv.Key, string.Join(",", kv.Value.Select(r => r.ToString()).ToArray()));
                                }
                            }
                        }

                        sb.Append(sbDetail.ToString());
                        sb.AppendFormat("\tBroker as leader distribution======={0}=======\r\n", topic);
                        sb.AppendFormat("\r\tBrokerID\tLeadPartition count\r\n");
                        foreach (KeyValuePair<int, int> kv in parttionBrokerID_LeaderCountDistrib)
                        {
                            sb.AppendFormat("\t\t{0}\t{1}\r\n", KafkaConsoleUtil.GetBrokerIDAndIP(kv.Key), kv.Value);
                        }

                        sb.AppendFormat("\tBroker as replica distribution========={0}=====\r\n", topic);
                        sb.AppendFormat("\r\tBrokerID\tReplication count count\r\n");
                        foreach (KeyValuePair<int, int> kv in parttionBrokerID_ReplicaCountDistrib)
                        {
                            sb.AppendFormat("\t\t{0}\t{1}\r\n", KafkaConsoleUtil.GetBrokerIDAndIP(kv.Key), kv.Value);
                        }

                        sb.AppendFormat("\r\n");
                    }
                }

                s = sb.ToString();
            }
            catch (NoBrokerForTopicException e)
            {
                sb.AppendFormat("\r\nTopic:{0}\t ==NoBrokerForTopicException:{1}!!!== \r\n", topic, e.Message);
                s = sb.ToString();
            }
            catch (UnableToConnectToHostException e)
            {
                sb.AppendFormat("\r\nTopic:{0}\t ==UnableToConnectToHostException:{1}!!!== \r\n", topic, e.Message);
                s = sb.ToString();
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Dump topic got exception:{0}\r\ninput parameter:Topic:{1}\tZookeeper:{2}\tKafka:{3}\tPartionIndex:{4}\tincludePartitionDetailInfo:{5}\tincludeOffsetInfo:{6}\ttimestamp:{7}\r\nPartial result:{8}"
                     , ExceptionUtil.GetExceptionDetailInfo(ex),
                     topic,
                     zookeeper,
                     string.Empty,
                     partitionIndex,
                     includePartitionDetailInfo,
                     includeOffsetInfo,
                     timestamp,
                     s);
            }

            return s;
        }

        //{"controller_epoch":4,"leader":2,"version":1,"leader_epoch":5,"isr":[2]}
        internal static ArrayList GetIsr(ZooKeeperClient zkClient, string topic, int partition)
        {
            string data = zkClient.ReadData<string>(string.Format("/brokers/topics/{0}/partitions/{1}/state"
                , topic, partition), true);
            Dictionary<string, object> ctx = new JavaScriptSerializer().Deserialize<Dictionary<string, object>>(data);
            Type ty = ctx["isr"].GetType();
            return (ArrayList)ctx["isr"];
        }
    }
}
