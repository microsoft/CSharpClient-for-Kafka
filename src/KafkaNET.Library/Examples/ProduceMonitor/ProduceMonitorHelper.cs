// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Cfg;
    using Kafka.Client.ZooKeeperIntegration;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class ProduceMonitorHelper
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ProduceMonitorHelper));

        private static SortedDictionary<int, long> latestOffsetDictLastValue = null;

        internal static void Run(ProduceMonitorHelperOptions produceMonitorOptions)
        {
            using (ZooKeeperClient zkClient = new ZooKeeperClient(produceMonitorOptions.Zookeeper,
                         ZooKeeperConfiguration.DefaultSessionTimeout, ZooKeeperStringSerializer.Serializer))
            {
                zkClient.Connect();
                while (true)
                {
                    SortedDictionary<int, long> latestOffsetDict = new SortedDictionary<int, long>();
                    SortedDictionary<int, int> parttionBrokerID_LeaderCountDistrib = new SortedDictionary<int, int>();
                    //BrokerID -->Count of as replica
                    SortedDictionary<int, int> parttionBrokerID_ReplicaCountDistrib = new SortedDictionary<int, int>();
                    SortedDictionary<int, long> latestLength = new SortedDictionary<int, long>();
                    TopicHelper.DumpTopicMetadataAndOffsetInternal(zkClient, produceMonitorOptions.Topic,
                    produceMonitorOptions.Zookeeper,
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
                        KafkaNetLibraryExample.AppendLineToFile(produceMonitorOptions.File, sb.ToString());
                        sb = new StringBuilder();
                        sb.AppendFormat("====LatestOffset====\r\n");
                        foreach (KeyValuePair<int, long> kv in latestOffsetDict)
                        {
                            sb.AppendFormat("{0,-9} ", kv.Value);
                        }

                        Logger.Info(sb.ToString());
                        KafkaNetLibraryExample.AppendLineToFile(produceMonitorOptions.File, sb.ToString());
                    }
                    else
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.Append("Latest Delta: ");
                        foreach (KeyValuePair<int, long> kv in latestOffsetDictLastValue)
                        {
                            if (latestOffsetDict.ContainsKey(kv.Key))
                            {
                                sb.AppendFormat("{0,-9} ", latestOffsetDict[kv.Key] - kv.Value);
                            }
                            else
                            {
                                sb.AppendFormat("Latest:{0,-9} ", kv.Value);
                            }
                        }

                        foreach (KeyValuePair<int, long> kv in latestOffsetDict)
                        {
                            if (!latestOffsetDictLastValue.ContainsKey(kv.Key))
                                sb.AppendFormat("NewLatest:{0}-{1,-9} ", kv.Key, kv.Value);
                        }

                        Logger.Info(sb.ToString());
                        KafkaNetLibraryExample.AppendLineToFile(produceMonitorOptions.File, sb.ToString());
                    }

                    latestOffsetDictLastValue = latestOffsetDict;
                    Thread.Sleep(produceMonitorOptions.IntervalInSeconds * 1000);
                }
            }
        }
    }
}
