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
    using Messages;
    using Requests;
    using Responses;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using Utils;
    using ZooKeeperIntegration;

    /// <summary>
    /// Background thread worker class that is used to fetch data from a single broker
    /// </summary>
    internal class FetcherRunnable
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(FetcherRunnable));

        private readonly Broker _broker;
        private readonly ConsumerConfiguration _config;
        private readonly int _fetchBufferLength;
        private readonly Action<PartitionTopicInfo> _markPartitonWithError;
        private readonly string _name;
        private readonly IList<PartitionTopicInfo> _partitionTopicInfos;
        private readonly IConsumer _simpleConsumer;
        private readonly IZooKeeperClient _zkClient;
        private volatile bool _shouldStop;

        internal FetcherRunnable(string name, IZooKeeperClient zkClient, ConsumerConfiguration config, Broker broker, List<PartitionTopicInfo> partitionTopicInfos, Action<PartitionTopicInfo> markPartitonWithError)
        {
            _name = name;
            _zkClient = zkClient;
            _config = config;
            _broker = broker;
            _partitionTopicInfos = partitionTopicInfos;
            _fetchBufferLength = config.MaxFetchBufferLength;
            _markPartitonWithError = markPartitonWithError;
            _simpleConsumer = new Consumer(_config, broker.Host, broker.Port);
        }

        /// <summary>
        /// Method to be used for starting a new thread
        /// </summary>
        internal void Run()
        {
            foreach (PartitionTopicInfo partitionTopicInfo in _partitionTopicInfos)
            {
                Logger.InfoFormat("{0} start fetching topic: {1} part: {2} offset: {3} from {4}:{5}",
                            _name,
                            partitionTopicInfo.Topic,
                            partitionTopicInfo.PartitionId,
                            partitionTopicInfo.NextRequestOffset,
                            _broker.Host,
                            _broker.Port);
            }

            int reqId = 0;
            while (!_shouldStop && _partitionTopicInfos.Any())
            {
                try
                {
                    IEnumerable<PartitionTopicInfo> fetchablePartitionTopicInfos = _partitionTopicInfos.Where(pti => pti.GetMessagesCount() < _fetchBufferLength);

                    long read = 0;

                    if (fetchablePartitionTopicInfos.Any())
                    {
                        FetchRequestBuilder builder =
                            new FetchRequestBuilder().
                                CorrelationId(reqId).
                                ClientId(_config.ConsumerId ?? _name).
                                MaxWait(_config.MaxFetchWaitMs).
                                MinBytes(_config.FetchMinBytes);
                        fetchablePartitionTopicInfos.ForEach(pti => builder.AddFetch(pti.Topic, pti.PartitionId, pti.NextRequestOffset, _config.FetchSize));

                        FetchRequest fetchRequest = builder.Build();
                        Logger.Debug("Sending fetch request: " + fetchRequest);
                        FetchResponse response = _simpleConsumer.Fetch(fetchRequest);
                        Logger.Debug("Fetch request completed");
                        var partitonsWithErrors = new List<PartitionTopicInfo>();
                        foreach (PartitionTopicInfo partitionTopicInfo in fetchablePartitionTopicInfos)
                        {
                            BufferedMessageSet messages = response.MessageSet(partitionTopicInfo.Topic, partitionTopicInfo.PartitionId);
                            switch (messages.ErrorCode)
                            {
                                case (short)ErrorMapping.NoError:
                                    int bytesRead = partitionTopicInfo.Add(messages);
                                    // TODO: The highwater offset on the message set is the end of the log partition. If the message retrieved is -1 of that offset, we are at the end.
                                    if (messages.Messages.Any())
                                    {
                                        partitionTopicInfo.NextRequestOffset = messages.Messages.Last().Offset + 1;
                                        read += bytesRead;
                                    }
                                    else
                                    {
                                        Logger.DebugFormat("No message returned by FetchRequest: {0}", fetchRequest.ToString());
                                    }
                                    break;
                                case (short)ErrorMapping.OffsetOutOfRangeCode:
                                    try
                                    {
                                        Logger.InfoFormat("offset for {0} out of range", partitionTopicInfo);
                                        long resetOffset = ResetConsumerOffsets(partitionTopicInfo.Topic,
                                                                                partitionTopicInfo.PartitionId);
                                        if (resetOffset >= 0)
                                        {
                                            partitionTopicInfo.ResetOffset(resetOffset);
                                            Logger.InfoFormat("{0} marked as done.", partitionTopicInfo);
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Logger.ErrorFormat("Error getting offsets for partition {0} : {1}", partitionTopicInfo.PartitionId, ex.FormatException());
                                        partitonsWithErrors.Add(partitionTopicInfo);
                                    }
                                    break;
                                default:
                                    Logger.ErrorFormat("Error returned from broker {2} for partition {0} : KafkaErrorCode: {1}", partitionTopicInfo.PartitionId, messages.ErrorCode, partitionTopicInfo.BrokerId);
                                    partitonsWithErrors.Add(partitionTopicInfo);
                                    break;

                            }
                        }

                        reqId = reqId == int.MaxValue ? 0 : reqId + 1;
                        if (partitonsWithErrors.Any())
                        {
                            RemovePartitionsFromProessing(partitonsWithErrors);
                        }
                    }

                    if (read > 0)
                    {
                        Logger.Debug("Fetched bytes: " + read);
                    }

                    if (read == 0)
                    {
                        Logger.DebugFormat("backing off {0} ms", _config.BackOffIncrement);
                        Thread.Sleep(_config.BackOffIncrement);
                    }

                }
                catch (Exception ex)
                {
                    if (_shouldStop)
                    {
                        Logger.InfoFormat("FetcherRunnable {0} interrupted", this);
                    }
                    else
                    {
                        Logger.ErrorFormat("error in FetcherRunnable {0}", ex.FormatException());
                    }
                }
            }

            Logger.InfoFormat("stopping fetcher {0} to host {1}", _name, _broker.Host);
        }

        private void RemovePartitionsFromProessing(List<PartitionTopicInfo> partitonsWithErrors)
        {
            foreach (var partitonWithError in partitonsWithErrors)
            {
                _partitionTopicInfos.Remove(partitonWithError);
                _markPartitonWithError(partitonWithError);
            }
        }

        internal void Shutdown()
        {
            _shouldStop = true;
        }

        private long ResetConsumerOffsets(string topic, int partitionId)
        {
            long offset;
            switch (_config.AutoOffsetReset)
            {
                case OffsetRequest.SmallestTime:
                    offset = OffsetRequest.EarliestTime;
                    break;
                case OffsetRequest.LargestTime:
                    offset = OffsetRequest.LatestTime;
                    break;
                default:
                    return 0;
            }

            var requestInfo = new Dictionary<string, List<PartitionOffsetRequestInfo>>();
            requestInfo[topic] = new List<PartitionOffsetRequestInfo> { new PartitionOffsetRequestInfo(partitionId, offset, 1) };

            var request = new OffsetRequest(requestInfo);
            OffsetResponse offsets = _simpleConsumer.GetOffsetsBefore(request);
            var topicDirs = new ZKGroupTopicDirs(_config.GroupId, topic);
            long offsetFound = offsets.ResponseMap[topic].First().Offsets[0];
            Logger.InfoFormat("updating partition {0} with {1} offset {2}", partitionId, offset == OffsetRequest.EarliestTime ? "earliest" : "latest", offsetFound);
            ZkUtils.UpdatePersistentPath(_zkClient, topicDirs.ConsumerOffsetDir + "/" + partitionId, offsetFound.ToString(CultureInfo.InvariantCulture));

            return offsetFound;
        }

        ~FetcherRunnable()
        {
            if (_simpleConsumer != null)
            {
                _simpleConsumer.Dispose();
            }
        }
    }
}