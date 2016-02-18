// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client;
    using Kafka.Client.Cfg;
    using Kafka.Client.ZooKeeperIntegration;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using ZooKeeperNet;

    internal class ConsumeGroupMonitorHelper
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumeGroupMonitorHelper));

        private static bool danymicAllConsumeGroupTopic = false;

        internal static string GetComsumerGroupOffsetsAsLog(Dictionary<int, long> dict)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var kv in dict.OrderBy(r => r.Key))
            {
                sb.AppendFormat("Partition:{0}\tOffset:{1,10}\r\n", kv.Key, kv.Value);
            }

            return sb.ToString();
        }

        internal static void DumpConsumerGroupOffsets(ConsumeGroupMonitorHelperOptions consumeGroupMonitorOption)
        {
            List<ConsumeGroupMonitorUnit> units = new List<ConsumeGroupMonitorUnit>();
            if (string.IsNullOrEmpty(consumeGroupMonitorOption.ConsumeGroupTopicArray))
            {
                if (!string.IsNullOrEmpty(consumeGroupMonitorOption.ConsumerGroupName))
                    units.Add(new ConsumeGroupMonitorUnit(consumeGroupMonitorOption.File, consumeGroupMonitorOption.ConsumerGroupName, consumeGroupMonitorOption.Topic));
                else
                {
                    danymicAllConsumeGroupTopic = true;
                }
            }
            else
            {
                string[] cgs = consumeGroupMonitorOption.ConsumeGroupTopicArray.Trim().Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var cg in cgs)
                {
                    string[] temp = cg.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                    if (temp.Length != 2)
                    {
                        Logger.ErrorFormat("Wrong parameter, Exmaple  G1:t1,G2:t2.  The value:{0} is invalid.", cg);
                    }
                    else
                        units.Add(new ConsumeGroupMonitorUnit(string.Format("{0}_{1}_{2}.txt", consumeGroupMonitorOption.File, temp[0], temp[1]), temp[0], temp[1]));
                }
            }
            long count = 0;
            int cycle = consumeGroupMonitorOption.RefreshConsumeGroupIntervalInSeconds / consumeGroupMonitorOption.IntervalInSeconds;
            using (ZooKeeperClient zkClient = new ZooKeeperClient(consumeGroupMonitorOption.Zookeeper,
                         ZooKeeperConfiguration.DefaultSessionTimeout, ZooKeeperStringSerializer.Serializer))
            {
                zkClient.Connect();
                while (true)
                {
                    DateTime time = DateTime.Now;
                    string dateFolder = string.Format("{0}_{1}_{2}", time.Year, time.Month, time.Day);
                    if (!Directory.Exists(dateFolder))
                        Directory.CreateDirectory(dateFolder);

                    if (danymicAllConsumeGroupTopic && count % cycle == 0)
                    {
                        units = new List<ConsumeGroupMonitorUnit>();
                        RefreshConsumeGroup(consumeGroupMonitorOption, zkClient, units);
                    }

                    int countSuccess = 0;
                    foreach (var unit in units)
                    {
                        unit.subFolder = dateFolder;
                        if (unit.Run(zkClient, consumeGroupMonitorOption.Zookeeper))
                            countSuccess++;
                    }

                    if (countSuccess == units.Count)
                        Logger.InfoFormat("===========All {0} consume group PASS=================", countSuccess);
                    else
                        Logger.ErrorFormat("===========All {0} consume group only {1} succ.  FAIL=================", units.Count, countSuccess);

                    Thread.Sleep(consumeGroupMonitorOption.IntervalInSeconds * 1000);
                    count++;

                }
            }
        }

        private static void RefreshConsumeGroup(ConsumeGroupMonitorHelperOptions consumeGroupMonitorOption, ZooKeeperClient zkClient, List<ConsumeGroupMonitorUnit> units)
        {
            Logger.Info("Will refresh all consume group from zk ...");
            string path = "/consumers";
            //TODO: add /users
            IEnumerable<string> groups = zkClient.GetChildren(path);
            foreach (var g in groups)
            {
                Logger.InfoFormat("Will check group {0}", g);
                string topicpath = string.Format("/consumers/{0}/owners", g);
                IEnumerable<string> topics = zkClient.GetChildren(topicpath);
                foreach (var t in topics)
                {
                    Logger.InfoFormat("Will check topic {0}", t);
                    units.Add(new ConsumeGroupMonitorUnit(string.Format("{0}_{1}_{2}.txt", consumeGroupMonitorOption.File, g, t), g, t));
                }
            }

            Logger.InfoFormat("Finsh refresh all consume group from zk ...{0}", units.Count);
        }
    }

    internal class ConsumeGroupMonitorUnit
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumeGroupMonitorUnit));

        public string file;
        public string subFolder;
        public string group;
        public string topic;

        private DateTime startTime = DateTime.UtcNow;
        private SortedDictionary<int, long> latestOffsetDictLastValue = null;
        private SortedDictionary<int, long> latestCommitedDictLastValue = null;
        private SortedDictionary<int, long> latestOffsetDictFirstValue = null;
        private SortedDictionary<int, long> latestCommitedDictFirstValue = null;
        private SortedDictionary<int, string> previousOwners = null;

        public ConsumeGroupMonitorUnit(string f, string g, string t)
        {
            this.file = f;
            this.group = g;
            this.topic = t;
        }

        public string GetFile()
        {
            if (string.IsNullOrEmpty(subFolder))
                return file;
            else
                return this.subFolder + "\\" + this.file;
        }

        internal static SortedDictionary<int, long> GetComsumerGroupOffsets(ZooKeeperClient zkClient,
            string topic, string consumerGroupName)
        {
            SortedDictionary<int, long> partitionOffset = new SortedDictionary<int, long>();

            string path = string.Format("/consumers/{0}/offsets/{1}"
                    , consumerGroupName, topic);

            IEnumerable<string> partitions = zkClient.GetChildrenParentMayNotExist(path);
            if (partitions != null)
            {
                foreach (var p in partitions)
                {
                    string fullPatht = string.Format("/consumers/{0}/offsets/{1}/{2}"
                    , consumerGroupName, topic, p);
                    string data = zkClient.ReadData<string>(fullPatht, true);
                    partitionOffset.Add(Convert.ToInt32(p), Convert.ToInt64(data));
                }
            }

            return partitionOffset;
        }

        internal static SortedDictionary<int, string> GetComsumerGroupOwners(ZooKeeperClient zkClient,
         string topic, string consumerGroupName)
        {
            SortedDictionary<int, string> partitionsOwners = new SortedDictionary<int, string>();

            string path = string.Format("/consumers/{0}/owners/{1}"
                    , consumerGroupName, topic);

            IEnumerable<string> partitions = zkClient.GetChildrenParentMayNotExist(path);
            if (partitions != null)
            {
                foreach (var p in partitions)
                {
                    string fullPatht = string.Format("/consumers/{0}/owners/{1}/{2}"
                    , consumerGroupName, topic, p);
                    string data = zkClient.ReadData<string>(fullPatht, true);
                    partitionsOwners.Add(Convert.ToInt32(p), data);
                }
            }

            return partitionsOwners;
        }

        internal bool Run(ZooKeeperClient zkClient, string zookeeper)
        {
            bool success = true;
            SortedDictionary<int, long> latestCommited = GetComsumerGroupOffsets(zkClient, this.topic, this.group);
            SortedDictionary<int, long> latestOffsetDict = new SortedDictionary<int, long>();
            SortedDictionary<int, int> parttionBrokerID_LeaderCountDistrib = new SortedDictionary<int, int>();
            //BrokerID -->Count of as replica
            SortedDictionary<int, int> parttionBrokerID_ReplicaCountDistrib = new SortedDictionary<int, int>();
            SortedDictionary<int, long> latestLength = new SortedDictionary<int, long>();
            //Owner
            SortedDictionary<int, string> latestOwners = GetComsumerGroupOwners(zkClient, this.topic, this.group);

            TopicHelper.DumpTopicMetadataAndOffsetInternal(zkClient, this.topic,
            zookeeper,
            -1,
            true,
            true,
            DateTime.MinValue,
            parttionBrokerID_LeaderCountDistrib, parttionBrokerID_ReplicaCountDistrib, latestOffsetDict, latestLength);

            if (latestOffsetDictLastValue == null)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("====Partitions====\r\n");
                foreach (KeyValuePair<int, long> kv in latestOffsetDict)
                {
                    sb.AppendFormat("{0,-9} ", kv.Key);
                }

                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());

                sb = new StringBuilder();
                sb.AppendFormat("====LatestOffset====\r\n");
                foreach (KeyValuePair<int, long> kv in latestOffsetDict)
                {
                    sb.AppendFormat("{0,-9} ", kv.Value);
                }

                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());

                sb = new StringBuilder();
                sb.AppendFormat("====ConsumedOffset====\r\n");
                foreach (KeyValuePair<int, long> kv in latestCommited)
                {
                    sb.AppendFormat("{0,-9} ", kv.Value);
                }

                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());

                sb = new StringBuilder();
                sb.AppendFormat("===Latest-Earliest:  Initial value====\r\n");
                foreach (KeyValuePair<int, long> kv in latestLength)
                {
                    sb.AppendFormat("{0,-9} ", kv.Value);
                }

                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());


                sb = new StringBuilder();
                sb.AppendFormat("====Latest-Commited:  Initial Value====\r\n");
                foreach (KeyValuePair<int, long> kv in latestOffsetDict)
                {
                    if (latestCommited.ContainsKey(kv.Key))
                        sb.AppendFormat("{0,-9} ", kv.Value - latestCommited[kv.Key]);
                    else
                        sb.AppendFormat("NotComiited:{0}-{1,-9} ", kv.Key, kv.Value);
                }

                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());

                sb = new StringBuilder();
                if (latestOffsetDict.Count == latestOwners.Count)
                {
                    sb.AppendFormat("====Owners== all {0} partitions has owner. ==\r\n", latestOwners.Count);
                }
                else
                {
                    sb.AppendFormat("====Owners ERROR. partitoins: {0} partition has owner: {1} ====\r\n", latestOffsetDict.Count, latestOwners.Count);
                }

                foreach (var ownerByOwnerName in (from o in latestOwners
                                                  group o by o.Value into g
                                                  select new
                                                  {
                                                      owner = g.Key,
                                                      partitions = g.ToArray()
                                                  }).OrderBy(r => r.owner))
                {
                    sb.AppendFormat("{0}:\t{1}\t", ownerByOwnerName.owner, ownerByOwnerName.partitions.Length);
                    for (int k = 0; k < ownerByOwnerName.partitions.Length; k++)
                    {
                        if (k == 0)
                            sb.AppendFormat("{0}", ownerByOwnerName.partitions[k]);
                        else
                            sb.AppendFormat(",   {0}", ownerByOwnerName.partitions[k]);
                    }

                    sb.AppendFormat("\r\n");
                }
                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendToFile(GetFile(), sb.ToString());

            }
            else
            {
                //Length
                StringBuilder sb = new StringBuilder();
                sb.Append("Latest-Earliest: ");
                foreach (KeyValuePair<int, long> kv in latestLength)
                {
                    sb.AppendFormat("{0,-9} ", kv.Value);
                }
                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());

                //Latest Delta
                sb = new StringBuilder();
                long latestDelta = 0;
                long aggregateLatestDelta = 0;
                foreach (KeyValuePair<int, long> kv in latestOffsetDictLastValue)
                {
                    if (latestOffsetDict.ContainsKey(kv.Key))
                    {
                        sb.AppendFormat("{0,-9} ", latestOffsetDict[kv.Key] - kv.Value);
                        latestDelta += latestOffsetDict[kv.Key] - kv.Value;
                    }
                    else
                    {
                        sb.AppendFormat("Latest:{0,-9} ", kv.Value);
                    }

                    if (latestOffsetDictFirstValue.ContainsKey(kv.Key))
                    {
                        aggregateLatestDelta += kv.Value - latestOffsetDictFirstValue[kv.Key];
                    }
                }

                foreach (KeyValuePair<int, long> kv in latestOffsetDict)
                {
                    if (!latestOffsetDictLastValue.ContainsKey(kv.Key))
                        sb.AppendFormat("NewLatest:{0}-{1,-9} ", kv.Key, kv.Value);
                }

                sb.Insert(0, string.Format("Latest Delta: {0}", latestDelta));
                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());

                //Commited Delta
                sb = new StringBuilder();
                long latestDeltaCommited = 0;
                long aggregateLatestCommite = 0;
                foreach (KeyValuePair<int, long> kv in latestCommitedDictLastValue)
                {
                    if (latestCommited.ContainsKey(kv.Key))
                    {
                        sb.AppendFormat("{0,-9} ", latestCommited[kv.Key] - kv.Value);
                        latestDeltaCommited += latestCommited[kv.Key] - kv.Value;
                    }
                    else
                    {
                        sb.AppendFormat("Commited:{0,-9} ", kv.Value);
                    }
                    if (latestCommitedDictFirstValue.ContainsKey(kv.Key))
                    {
                        aggregateLatestCommite += kv.Value - latestCommitedDictFirstValue[kv.Key];
                    }
                }

                foreach (KeyValuePair<int, long> kv in latestCommited)
                {
                    if (!latestCommitedDictLastValue.ContainsKey(kv.Key))
                        sb.AppendFormat("NewCommited:{0}-{1,-9} ", kv.Key, kv.Value);
                }

                sb.Insert(0, string.Format("Commited Delta: {0}", latestDeltaCommited));
                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());

                //Gap
                sb = new StringBuilder();
                sb.AppendFormat("Latest-Commited: {0}= ", latestOffsetDict.Count);
                foreach (KeyValuePair<int, long> kv in latestOffsetDict)
                {
                    if (latestCommited.ContainsKey(kv.Key))
                        sb.AppendFormat("{0,-9} ", kv.Value - latestCommited[kv.Key]);
                    else
                        sb.AppendFormat("NotComiited:{0}-{1,-9} ", kv.Key, kv.Value);
                }

                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());

                //Owner

                sb = new StringBuilder();
                if (latestOffsetDict.Count == latestOwners.Count)
                {
                    sb.AppendFormat("====Owners== all {0} partitions has owner. ==\r\n", latestOwners.Count);
                }
                else
                {
                    sb.AppendFormat("====Owners ERROR. partitoins: {0} partition has owner: {1} ====\r\n", latestOffsetDict.Count, latestOwners.Count);
                    success = false;
                }

                foreach (var ownerByOwnerName in (from o in latestOwners
                                                  group o by o.Value into g
                                                  select new
                                                  {
                                                      owner = g.Key,
                                                      partitions = g.ToArray()
                                                  }).OrderBy(r => r.owner))
                {
                    sb.AppendFormat("{0}:\t{1}\t", ownerByOwnerName.owner, ownerByOwnerName.partitions.Length);
                    for (int k = 0; k < ownerByOwnerName.partitions.Length; k++)
                    {
                        if (k == 0)
                            sb.AppendFormat("{0}", ownerByOwnerName.partitions[k]);
                        else
                            sb.AppendFormat(",   {0}", ownerByOwnerName.partitions[k]);
                    }
                    sb.AppendFormat("\r\n");
                }

                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendToFile(GetFile(), sb.ToString());
                sb = new StringBuilder();
                sb.AppendFormat("In last {0:0.0} seconds.  Totally latest offset change:{1}   Totally commited offset change:{2} . Percentage:{3:P2} Time:{4}\r\n"
                   , (DateTime.UtcNow - startTime).TotalSeconds, aggregateLatestDelta, aggregateLatestCommite, aggregateLatestCommite * 1.0 / aggregateLatestDelta, DateTime.Now);
                Logger.Info(sb.ToString());
                KafkaNetLibraryExample.AppendLineToFile(GetFile(), sb.ToString());
            }

            previousOwners = latestOwners;
            latestOffsetDictLastValue = latestOffsetDict;
            latestCommitedDictLastValue = latestCommited;
            if (latestOffsetDictFirstValue == null)
                latestOffsetDictFirstValue = latestOffsetDict;
            if (latestCommitedDictFirstValue == null)
                latestCommitedDictFirstValue = latestCommited;
            return success;
        }
    }
}