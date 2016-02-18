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
    using Kafka.Client.Exceptions;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using Utils;
    using ZooKeeperIntegration;

    /// <summary>
    /// Background thread that fetches data from a set of servers
    /// </summary>
    internal class Fetcher : IDisposable
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(Fetcher));

        private readonly ConsumerConfiguration _config;
        private readonly IZooKeeperClient _zkClient;
        private ConcurrentBag<FetcherRunnable> _fetcherWorkerObjects;
        private ConcurrentBag<Thread> _fetcherThreads;
        private Thread _leaderFinderThread;
        private volatile bool _disposed;
        private readonly object _shuttingDownLock = new object();
        private readonly ConcurrentQueue<PartitionTopicInfo> _partitionsNeedingLeaders = new ConcurrentQueue<PartitionTopicInfo>();
        private PartitionLeaderFinder _leaderFinder;

        /// <summary>
        /// Initializes a new instance of the <see cref="Fetcher" /> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration.
        /// </param>
        /// <param name="zkClient">
        /// The wrapper above ZooKeeper client.
        /// </param>
        public Fetcher(ConsumerConfiguration config, IZooKeeperClient zkClient)
        {
            _config = config;
            _zkClient = zkClient;
        }

        /// <summary>
        /// Shuts down all fetch threads
        /// </summary>
        public void Shutdown()
        {

            if (_fetcherWorkerObjects != null)
            {
                foreach (FetcherRunnable fetcherRunnable in _fetcherWorkerObjects)
                {
                    if (fetcherRunnable == null)
                    {
                        Logger.Error("Fetch Runnable is null!");
                    }
                    else
                    {
                        fetcherRunnable.Shutdown();
                    }
                }

                int threadsStillRunning = 0;
                Stopwatch stopWatch = Stopwatch.StartNew();
                bool shutdownTimeout = false;
                // make sure all fetcher threads stopped
                do
                {
                    Thread.Sleep(500);
                    threadsStillRunning = 0;
                    foreach (Thread fetcherThread in _fetcherThreads)
                    {
                        if (fetcherThread == null)
                        {
                            Logger.Error("Fetch thread is null!");
                        }
                        else
                        {
                            if (fetcherThread.IsAlive)
                            {
                                threadsStillRunning++;
                            }
                        }
                    }
                    if (stopWatch.ElapsedMilliseconds >= _config.ShutdownTimeout)
                    {
                        shutdownTimeout = true;
                    }
                } while (threadsStillRunning > 0 && !shutdownTimeout);

                stopWatch.Stop();
                if (shutdownTimeout)
                {
                    // BUG:1482409 - added timeout watch and forceful aborting of lingering background threads.
                    // shutdown exceeded timeout
                    Logger.Warn("All background fetcher threads did not shutdown in the specified amount of time. Raising abort exceptions to stop them.");
                    foreach (Thread fetcherThread in _fetcherThreads)
                    {
                        if (fetcherThread == null)
                        {
                            Logger.Error("Fetch thread is null!");
                        }
                        if (fetcherThread.IsAlive)
                        {
                            fetcherThread.Abort();
                        }
                    }
                }

                _leaderFinder.Stop();
                _leaderFinder = null;
                _leaderFinderThread = null;
                _fetcherWorkerObjects = null;
                _fetcherThreads = null;
            }
        }

        public void ClearFetcherQueues<TData>(IList<PartitionTopicInfo> topicInfos, Cluster cluster, IEnumerable<BlockingCollection<FetchedDataChunk>> queuesToBeCleared,
                                              IDictionary<string, IList<KafkaMessageStream<TData>>> kafkaMessageStreams)
        {
            if (kafkaMessageStreams != null)
            {
                foreach (KeyValuePair<string, IList<KafkaMessageStream<TData>>> kafkaMessageStream in kafkaMessageStreams)
                {
                    foreach (KafkaMessageStream<TData> stream in kafkaMessageStream.Value)
                    {
                        stream.Clear();
                    }
                }
            }

            Logger.Info("Cleared the data chunks in all the consumer message iterators");
            // Clear all but the currently iterated upon chunk in the consumer thread's queue
            foreach (BlockingCollection<FetchedDataChunk> queueToBeCleared in queuesToBeCleared)
            {
                while (queueToBeCleared.Count > 0)
                {
                    queueToBeCleared.Take();
                }
            }

            Logger.Info("Cleared all relevant queues for this fetcher");
        }

        /// <summary>
        /// Opens connections to brokers.
        /// </summary>
        /// <param name="topicInfos">
        /// The topic infos.
        /// </param>
        /// <param name="cluster">
        /// The cluster.
        /// </param>
        /// <param name="queuesToBeCleared">
        /// The queues to be cleared.
        /// </param>
        public void InitConnections(IEnumerable<PartitionTopicInfo> topicInfos, Cluster cluster)
        {
            EnsuresNotDisposed();
            Shutdown();
            if (topicInfos == null)
            {
                return;
            }
            var partitionTopicInfoMap = new Dictionary<int, List<PartitionTopicInfo>>();

            //// re-arrange by broker id
            foreach (PartitionTopicInfo topicInfo in topicInfos)
            {
                if (!partitionTopicInfoMap.ContainsKey(topicInfo.BrokerId))
                {
                    partitionTopicInfoMap.Add(topicInfo.BrokerId, new List<PartitionTopicInfo> { topicInfo });
                }
                else
                {
                    partitionTopicInfoMap[topicInfo.BrokerId].Add(topicInfo);
                }
            }

            _leaderFinder = new PartitionLeaderFinder(_partitionsNeedingLeaders, cluster, _config, CreateFetchThread);
            _leaderFinderThread = new Thread(_leaderFinder.Start);
            _leaderFinderThread.Start();

            //// open a new fetcher thread for each broker
            _fetcherWorkerObjects = new ConcurrentBag<FetcherRunnable>();
            _fetcherThreads = new ConcurrentBag<Thread>();
            int i = 0;
            foreach (KeyValuePair<int, List<PartitionTopicInfo>> item in partitionTopicInfoMap)
            {
                Broker broker = cluster.GetBroker(item.Key);
                if (broker == null)
                {
                    foreach (PartitionTopicInfo p in item.Value)
                        AddPartitionWithError(p);
                    Logger.Error("Could not find broker associated with broker id: " + item.Key + " partitions: " + string.Join(",", item.Value.Select(r => string.Format("Topic:{0} PartitionsID:{1} ", r.Topic, r.PartitionId)).ToArray()) + " will repeat retry ...");
                }
                else
                {
                    Logger.Debug("Found broker associated with broker id: " + item.Key + " partitions: " + string.Join(",", item.Value.Select(r => string.Format("Topic:{0} PartitionsID:{1} ", r.Topic, r.PartitionId)).ToArray()) + " will create fetch threads ...");
                    CreateFetchThread(item.Value, broker);
                }
                i++;
            }
        }

        private void CreateFetchThread(PartitionTopicInfo partition, Broker broker)
        {
            if (_fetcherThreads == null)
            {
                return;
            }

            CreateFetchThread(new List<PartitionTopicInfo>() { partition }, broker);
        }

        private void CreateFetchThread(List<PartitionTopicInfo> partitions, Broker broker)
        {
            if (_disposed)
            {
                return;
            }

            Logger.DebugFormat("Creating Fetcher on broker {0} for partitions: {1}", broker.Id, string.Join(",", partitions.Select(p => string.Format("{0}({1})", p.Topic, p.PartitionId))));
            var fetcherRunnable = new FetcherRunnable("FetcherRunnable-" + _fetcherWorkerObjects.Count, _zkClient, _config, broker, partitions, AddPartitionWithError);
            var threadStart = new ThreadStart(fetcherRunnable.Run);
            var fetcherThread = new Thread(threadStart);
            _fetcherWorkerObjects.Add(fetcherRunnable);
            fetcherThread.Name = string.Format("FetcherThread_broker_{0}_partitions_{1}", broker.Id, string.Join(",", partitions.Select(p => string.Format("{0}({1})", p.Topic, p.PartitionId))));
            _fetcherThreads.Add(fetcherThread);
            fetcherThread.Start();
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            lock (_shuttingDownLock)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
            }

            try
            {
                Shutdown();
            }
            catch (Exception exc)
            {
                Logger.WarnFormat("Ignoring unexpected errors on closing", exc.FormatException());
            }
        }

        internal void AddPartitionWithError(PartitionTopicInfo partition)
        {
            _partitionsNeedingLeaders.Enqueue(partition);
        }

        /// <summary>
        /// Ensures that object was not disposed
        /// </summary>
        private void EnsuresNotDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name);
            }
        }
    }
}
