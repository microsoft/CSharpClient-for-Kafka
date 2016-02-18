// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.ZooKeeperIntegration.Listeners
{
    using Kafka.Client.Utils;
    using log4net;
    using System;
    using System.Collections.Generic;
    using System.Web.Script.Serialization;

    internal class ZkPartitionLeaderListener<TData> : IZooKeeperDataListener
    {
        public static ILog Logger { get { return LogManager.GetLogger(typeof(ZkPartitionLeaderListener<TData>)); } }

        private ZKRebalancerListener<TData> _rebalancer;
        private Dictionary<string, int> _partitionLeaderMap;

        public ZkPartitionLeaderListener(ZKRebalancerListener<TData> rebalancer, Dictionary<string, int> partitionLeaderMap = null)
        {
            _rebalancer = rebalancer;
            if (partitionLeaderMap != null)
            {
                _partitionLeaderMap = new Dictionary<string, int>(partitionLeaderMap);
            }
            else
            {
                _partitionLeaderMap = new Dictionary<string, int>();
            }
        }

        public void HandleDataChange(Events.ZooKeeperDataChangedEventArgs args)
        {
            int parsedLeader;
            string nodePath = args.Path;
            string nodeData = args.Data;

            Logger.Info("A partition leader or ISR list has been updated. Determining if rebalancing is necessary");
            if (!ZkUtils.TryParsePartitionLeader(nodeData, out parsedLeader))
            {
                Logger.Error("Skipping rebalancing. Failed to parse partition leader for path: " + nodePath + " from ZK node");
                return;
            }
            else
            {
                if (!_partitionLeaderMap.ContainsKey(nodePath) || _partitionLeaderMap[nodePath] != parsedLeader)
                {
                    string currentLeader = _partitionLeaderMap.ContainsKey(nodePath) ? _partitionLeaderMap[nodePath].ToString() : "null";
                    Logger.Info("Performing rebalancing. Leader value for path: " + nodePath + " has changed from " + currentLeader + " to " + parsedLeader);
                    _partitionLeaderMap[nodePath] = parsedLeader;
                    _rebalancer.AsyncRebalance();
                }
                else
                {
                    Logger.Info("Skipping rebalancing. Leader value for path: " + nodePath + " is " + parsedLeader.ToString() + " and has not changed");
                }
            }
        }

        public void HandleDataDelete(Events.ZooKeeperDataChangedEventArgs args)
        {
            _rebalancer.AsyncRebalance();
        }
    }
}