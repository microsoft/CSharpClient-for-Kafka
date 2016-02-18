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

namespace Kafka.Client.Consumers
{
    using Cfg;
    using Cluster;
    using Producers;
    using Requests;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using Utils;

    internal class PartitionLeaderFinder
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(PartitionLeaderFinder));

        private static readonly int FailureRetryDelayMs = (int)TimeSpan.FromSeconds(5).TotalMilliseconds;

        private const string clientId = "LeaderFetcher";

        private readonly Cluster _brokers;

        private readonly ConsumerConfiguration _config;

        private readonly Action<PartitionTopicInfo, Broker> _createNewFetcher;

        private readonly ConcurrentQueue<PartitionTopicInfo> _partitionsNeedingLeader;

        private volatile bool _stop;

        public PartitionLeaderFinder(ConcurrentQueue<PartitionTopicInfo> partitionsNeedingLeaders, Cluster brokers, ConsumerConfiguration config, Action<PartitionTopicInfo, Broker> createNewFetcher)
        {
            _partitionsNeedingLeader = partitionsNeedingLeaders;
            _brokers = brokers;
            _config = config;
            _createNewFetcher = createNewFetcher;
        }

        public void Start()
        {
            while (!_stop)
            {
                try
                {
                    if (_partitionsNeedingLeader.IsEmpty)
                    {
                        Thread.Sleep(_config.ConsumeGroupFindNewLeaderSleepIntervalMs);
                        continue;
                    }

                    PartitionTopicInfo partition;
                    if (_partitionsNeedingLeader.TryDequeue(out partition))
                    {
                        Logger.DebugFormat("Finding new leader for topic {0}, partition {1}", partition.Topic, partition.PartitionId);
                        Broker newLeader = null;
                        foreach (KeyValuePair<int, Broker> broker in _brokers.Brokers)
                        {
                            var consumer = new Consumer(_config, broker.Value.Host, broker.Value.Port);
                            try
                            {
                                IEnumerable<TopicMetadata> metaData = consumer.GetMetaData(TopicMetadataRequest.Create(new[] { partition.Topic }, 1, 0, clientId));
                                if (metaData != null && metaData.Any())
                                {
                                    PartitionMetadata newPartitionData = metaData.First().PartitionsMetadata.FirstOrDefault(p => p.PartitionId == partition.PartitionId);
                                    if (newPartitionData != null)
                                    {
                                        Logger.DebugFormat("New leader for {0} ({1}) is broker {2}", partition.Topic, partition.PartitionId, newPartitionData.Leader.Id);
                                        newLeader = newPartitionData.Leader;
                                        break;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                Logger.WarnFormat("Error retrieving meta data from broker {0}: {1}", broker.Value.Id, ex.FormatException());
                            }
                        }

                        if (newLeader == null)
                        {
                            Logger.ErrorFormat("New leader information could not be retrieved for {0} ({1})", partition.Topic, partition.PartitionId);
                        }
                        else
                        {
                            _createNewFetcher(partition, newLeader);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.ErrorFormat("PartitionLeaderFinder encountered an error: {0}", ex.FormatException());
                    Thread.Sleep(FailureRetryDelayMs);
                }
            }

            Logger.Info("Partition leader finder thread shutting down.");
        }

        public void Stop()
        {
            _stop = true;
        }
    }
}