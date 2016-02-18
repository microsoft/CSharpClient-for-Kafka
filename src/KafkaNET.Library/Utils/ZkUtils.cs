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
    using Kafka.Client.Cluster;
    using Kafka.Client.Exceptions;
    using Kafka.Client.ZooKeeperIntegration;
    using log4net;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Web.Script.Serialization;
    using ZooKeeperNet;

    public class ZkUtils
    {
        public static log4net.ILog Logger= log4net.LogManager.GetLogger(typeof(ZkUtils)); 
        internal const string BrokerTopicsPath = "/brokers/topics";

        //[zk: localhost(CONNECTED) 12] get /brokers/topics/mvlogs
        //{"version":1,"partitions":{"1":[3,2],"0":[2,3]}}
        public static Dictionary<int, int[]> GetTopicMetadataInzookeeper(ZooKeeperClient zkClient, string topic)
        {
            Dictionary<int, int[]> treturn = new Dictionary<int, int[]>();

            try
            {
                string data = zkClient.ReadData<string>(string.Format("/brokers/topics/{0}", topic), true);
                Dictionary<string, object> ctx = new JavaScriptSerializer().Deserialize<Dictionary<string, object>>(data);
                Type ty = ctx["partitions"].GetType();
                //Logger.InfoFormat("The type for partitions :{0}", ty.FullName);
                Dictionary<string, object> tpartitons = (Dictionary<string, object>)ctx["partitions"];

                foreach (KeyValuePair<string, object> kv in tpartitons)
                {
                    int partitionID = Convert.ToInt32(kv.Key);
                    //Logger.InfoFormat("The type for partitions value :{0}", kv.Value.GetType().FullName);
                    ArrayList rep = (ArrayList)kv.Value;
                    int[] partitionReplicas = new int[rep.Count];

                    for (int i = 0; i < rep.Count; i++)
                        partitionReplicas[i] = Convert.ToInt32(rep[i]);
                    treturn.Add(partitionID, partitionReplicas);
                }
                Logger.InfoFormat("Get topic data directly from zookeeper Topic:{0} Data:{1} Partition count:{2}", topic, data, treturn.Count);
            }
            catch (Exception ex)
            {
                Logger.Error("Failed to get topic " + topic + " data directly from zookeeper: " + ex.FormatException());
            }

            return treturn;
        }

        internal static void UpdatePersistentPath(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                zkClient.WriteData(path, data);
            }
            catch (KeeperException e)
            {
                if (e.ErrorCode == KeeperException.Code.NONODE)
                {
                    CreateParentPath(zkClient, path);

                    try
                    {
                        zkClient.CreatePersistent(path, data);
                    }
                    catch (KeeperException e2)
                    {
                        if (e2.ErrorCode == KeeperException.Code.NODEEXISTS)
                            zkClient.WriteData(path, data);
                        else
                            throw;
                    }
                }
                else
                    throw;
            }
        }

        internal static void CreateParentPath(IZooKeeperClient zkClient, string path)
        {
            string parentDir = path.Substring(0, path.LastIndexOf('/'));
            if (parentDir.Length != 0)
            {
                zkClient.CreatePersistent(parentDir, true);
            }
        }

        internal static string GetConsumerPartitionOwnerPath(string group, string topic, string partition)
        {
            var topicDirs = new ZKGroupTopicDirs(group, topic);
            return topicDirs.ConsumerOwnerDir + "/" + partition;
        }

        internal static string GetConsumerPartitionOffsetPath(string group, string topic, string partition)
        {
            var topicDirs = new ZKGroupTopicDirs(group, topic);
            return topicDirs.ConsumerOffsetDir + "/" + partition;
        }

        internal static void DeletePath(IZooKeeperClient zkClient, string path)
        {
            try
            {
                zkClient.Delete(path);
            }
            catch (KeeperException e)
            {
                if (e.ErrorCode == KeeperException.Code.NONODE)
                    Logger.InfoFormat("{0} deleted during connection loss; this is ok", path);
                else
                    throw;
            }
        }

        internal static IDictionary<string, IList<string>> GetPartitionsForTopics(IZooKeeperClient zkClient, IEnumerable<string> topics)
        {
            var result = new Dictionary<string, IList<string>>();
            foreach (string topic in topics)
            {
                var partitions = zkClient.GetChildrenParentMayNotExist(GetTopicPartitionsPath(topic));

                if (partitions == null)
                {
                    throw new NoPartitionsForTopicException(topic);
                }

                Logger.DebugFormat("children of /brokers/topics/{0} are {1}", topic, string.Join(",", partitions));
                result.Add(topic, partitions != null ? partitions.OrderBy(x => x).ToList() : new List<string>());
            }

            return result;
        }

        internal static string GetTopicPath(string topic)
        {
            return BrokerTopicsPath + "/" + topic;
        }

        internal static void CreateEphemeralPathExpectConflict(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                CreateEphemeralPath(zkClient, path, data);
            }
            catch (KeeperException e)// KeeperException.NoNodeException)
            {
                if (e.ErrorCode == KeeperException.Code.NODEEXISTS)
                {
                    string storedData;
                    try
                    {
                        storedData = zkClient.ReadData<string>(path);
                    }
                    catch (Exception)
                    {
                        // the node disappeared; treat as if node existed and let caller handles this
                        throw;
                    }

                    if (storedData == null || storedData != data)
                    {
                        Logger.InfoFormat("conflict in {0} data: {1} stored data: {2}", path, data, storedData);
                        throw;
                    }
                    else
                    {
                        // otherwise, the creation succeeded, return normally
                        Logger.InfoFormat("{0} exits with value {1} during connection loss; this is ok", path, data);
                    }
                }
                else
                    throw;
            }
        }

        internal static void CreateEphemeralPath(IZooKeeperClient zkClient, string path, string data)
        {
            try
            {
                zkClient.CreateEphemeral(path, data);
            }
            catch (KeeperException e)
            {
                if (e.ErrorCode == KeeperException.Code.NONODE)
                {
                    ZkUtils.CreateParentPath(zkClient, path);
                    zkClient.CreateEphemeral(path, data);
                }
                else
                    throw;
            }
        }

        public static IEnumerable<Broker> GetAllBrokersInCluster(IZooKeeperClient zkClient)
        {
            var brokerIds = zkClient.GetChildren(ZooKeeperClient.DefaultBrokerIdsPath).OrderBy(x => x).ToList();
            return ZkUtils.GetBrokerInfoFromIds(zkClient, brokerIds.Select(x => int.Parse(x)));
        }

        internal static int? GetLeaderForPartition(IZooKeeperClient zkClient, string topic, int partition)
        {
            var stateData = zkClient.ReadData<string>(GetTopicPartitionStatePath(topic, partition.ToString(CultureInfo.InvariantCulture)), true);

            if (string.IsNullOrWhiteSpace(stateData))
            {
                return (int?)null;
            }
            else
            {
                int leader;
                return TryParsePartitionLeader(stateData, out leader) ? (int?)leader : null;
            }
        }

        public static IEnumerable<Broker> GetBrokerInfoFromIds(IZooKeeperClient zkClient, IEnumerable<int> brokerIds)
        {
            return brokerIds.Select(
                brokerId =>
                Broker.CreateBroker(brokerId,
                                    zkClient.ReadData<string>(ZooKeeperClient.DefaultBrokerIdsPath + "/" + brokerId)));
        }


        internal static TopicPartitionState GetPartitionState(IZooKeeperClient zkClient, string topic, int partition)
        {
            var stateData = zkClient.ReadData<string>(GetTopicPartitionStatePath(topic, partition.ToString(CultureInfo.InvariantCulture)), true);

            if (string.IsNullOrWhiteSpace(stateData))
            {
                return null;
            }

            TopicPartitionState partitionState;
            try
            {
                var ser = new JavaScriptSerializer();
                var result = ser.Deserialize<Dictionary<string, object>>(stateData);
                partitionState = new TopicPartitionState()
                                         {
                                             Leader = int.Parse(result["leader"].ToString()),
                                             Leader_Epoch = int.Parse(result["leader_epoch"].ToString()),
                                             Controller_Epoch = int.Parse(result["controller_epoch"].ToString()),
                                             Verstion = int.Parse(result["version"].ToString()),
                                         };

                var isrArr = result["isr"] as System.Collections.ArrayList;
                partitionState.Isr = isrArr != null ? isrArr.Cast<int>().ToArray() : null;
            }
            catch (Exception exc)
            {
                Logger.WarnFormat("Unexpected error while trying to get topic partition state for topic '{0}' partition '{1}'. Error: {2} ", topic, partition, exc.FormatException());
                return null;
            }

            return partitionState;
        }

        internal static bool TryParsePartitionLeader(string partitionState, out int leader)
        {
            leader = Int32.MinValue;
            bool success = true;

            try
            {
                // Parse leader value from partition state JSON string
                var ser = new JavaScriptSerializer();
                Dictionary<string, object> result = ser.Deserialize<Dictionary<string, object>>(partitionState);
                leader = (int)result["leader"];
            }
            catch (Exception ex)
            {
                Logger.Error("Failed to parse partition leader due to the following exception: " + ex.Message);
                success = false;
            }

            if (leader == Int32.MinValue)
            {
                Logger.ErrorFormat("Can't get leader from zookeeper data: {0}", partitionState);
                return false;
            }

            return success;
        }

        private static string GetTopicPartitionStatePath(string topic, string partitionId)
        {
            return GetTopicPartitionPath(topic, partitionId) + "/" + "state";
        }

        private static string GetTopicPartitionPath(string topic, string partitionId)
        {
            return GetTopicPartitionsPath(topic) + "/" + partitionId;
        }

        private static string GetTopicPartitionsPath(string topic)
        {
            return GetTopicPath(topic) + "/partitions";
        }

        private static string GetTopicPartitionReplicasPath(string topic, string partitionId)
        {
            return GetTopicPartitionPath(topic, partitionId) + "/" + "replicas";
        }

    }

    public class TopicPartitionState
    {
        public int Controller_Epoch { get; set; }
        public int[] Isr { get; set; }
        public int Leader { get; set; }
        public int Leader_Epoch { get; set; }

        public int Verstion { get; set; }
    }
}
