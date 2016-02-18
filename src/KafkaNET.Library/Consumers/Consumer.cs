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

using Kafka.Client.Responses;

namespace Kafka.Client.Consumers
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using log4net;
    using Producers;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Reflection;

    /// <summary>
    /// The low-level API of consumer of Kafka messages
    /// </summary>
    /// <remarks>
    /// Maintains a connection to a single broker and has a close correspondence
    /// to the network requests sent to the server.
    /// Also, is completely stateless.
    /// </remarks>
    public class Consumer : IConsumer
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(Consumer));

        private readonly ConsumerConfiguration config;
        private readonly string host;
        private readonly int port;

        private KafkaConnection connection;

        internal long CreatedTimeInUTC;
        public ConsumerConfiguration Config
        {
            get
            {
                return config;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Consumer"/> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration.
        /// </param>
        public Consumer(ConsumerConfiguration config)
        {
            Guard.NotNull(config, "config");

            this.config = config;
            this.host = config.Broker.Host;
            this.port = config.Broker.Port;
            this.connection = new KafkaConnection(
                                  this.host,
                                  this.port,
                                  this.config.BufferSize,
                                  this.config.SendTimeout,
                                  this.config.ReceiveTimeout,
                                  this.config.ReconnectInterval);
            this.CreatedTimeInUTC = DateTime.UtcNow.Ticks;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Consumer"/> class.
        /// </summary>
        /// <param name="config">
        /// The consumer configuration.
        /// </param>
        /// <param name="host"></param>
        /// <param name="port"></param>
        public Consumer(ConsumerConfiguration config, string host, int port)
        {
            Guard.NotNull(config, "config");

            this.config = config;
            this.host = host;
            this.port = port;
            this.connection = new KafkaConnection(
                                  this.host,
                                  this.port,
                                  this.config.BufferSize,
                                  this.config.SendTimeout,
                                  this.config.ReceiveTimeout,
                                  this.config.ReconnectInterval);
        }

        #region Fetch

        public FetchResponse Fetch(string clientId, string topic, int correlationId, int partitionId, long fetchOffset, int fetchSize
            , int maxWaitTime, int minWaitSize)
        {
            var requestMap = new Dictionary<string, List<PartitionFetchInfo>>();
            requestMap.Add(
                topic,
                new List<PartitionFetchInfo>()
                        {
                            new PartitionFetchInfo(
                                partitionId,
                                fetchOffset,
                                fetchSize)
                        });
            return this.Fetch(new FetchRequest(
                correlationId,
                clientId,
                maxWaitTime,
                minWaitSize,
                requestMap));
        }
        public FetchResponse Fetch(FetchRequest request)
        {
            short tryCounter = 1;
            while (tryCounter <= this.config.NumberOfTries)
            {
                try
                {
                    Logger.Debug("Fetch is waiting for send lock");
                    lock (this)
                    {
                        Logger.Debug("Fetch acquired send lock. Begin send");
                        return connection.Send(request);
                    }
                }
                catch (Exception ex)
                {
                    //// if maximum number of tries reached
                    if (tryCounter == this.config.NumberOfTries)
                    {
                        throw;
                    }

                    tryCounter++;
                    Logger.InfoFormat("Fetch reconnect due to {0}", ex.FormatException());
                }
            }

            return null;
        }

        /// <summary>
        /// MANIFOLD use
        /// </summary>
        public FetchResponseWrapper FetchAndGetDetail(string clientId, string topic, int correlationId, int partitionId, long fetchOffset, int fetchSize
            , int maxWaitTime, int minWaitSize)
        {
            FetchResponse response = this.Fetch(clientId,
                topic,
                correlationId,
                partitionId,
                fetchOffset,
                fetchSize,
                maxWaitTime,
                minWaitSize);
            if (response == null)
            {
                throw new KafkaConsumeException(string.Format("FetchRequest returned null FetchResponse,fetchOffset={0},leader={1},topic={2},partition={3}",
                    fetchOffset, this.Config.Broker, topic, partitionId));
            }
            PartitionData partitionData = response.PartitionData(topic, partitionId);
            if (partitionData == null)
            {
                throw new KafkaConsumeException(string.Format("PartitionData int FetchResponse is null,fetchOffset={0},leader={1},topic={2},partition={3}",
                    fetchOffset, this.Config.Broker, topic, partitionId));
            }
            if (partitionData.Error != ErrorMapping.NoError)
            {
                string s = string.Format("Partition data in FetchResponse has error. {0}  {1} fetchOffset={2},leader={3},topic={4},partition={5}"
                    , partitionData.Error, KafkaException.GetMessage(partitionData.Error)
                    , fetchOffset, this.Config.Broker, topic, partitionId);
                Logger.Error(s);
                throw new KafkaConsumeException(s, partitionData.Error);
            }
            return new FetchResponseWrapper(partitionData.GetMessageAndOffsets(), response.Size, response.CorrelationId, topic, partitionId);
        }
        #endregion

        /// <summary>
        /// Gets a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="request">
        /// The offset request.
        /// </param>
        /// <returns>
        /// The list of offsets, in descending order.
        /// </returns>
        public OffsetResponse GetOffsetsBefore(OffsetRequest request)
        {
            short tryCounter = 1;
            while (tryCounter <= this.config.NumberOfTries)
            {
                try
                {
                    lock (this)
                    {
                        return connection.Send(request);
                    }
                }
                catch (Exception ex)
                {
                    //// if maximum number of tries reached
                    if (tryCounter == this.config.NumberOfTries)
                    {
                        throw;
                    }

                    tryCounter++;
                    Logger.InfoFormat("GetOffsetsBefore reconnect due to {0}", ex.FormatException());
                }
            }

            return null;
        }

        public IEnumerable<TopicMetadata> GetMetaData(TopicMetadataRequest request)
        {
            short tryCounter = 1;
            while (tryCounter <= this.config.NumberOfTries)
            {
                try
                {
                    lock (this)
                    {
                        return connection.Send(request);
                    }
                }
                catch (Exception ex)
                {
                    //// if maximum number of tries reached
                    if (tryCounter == this.config.NumberOfTries)
                    {
                        throw;
                    }

                    tryCounter++;
                    Logger.InfoFormat("GetMetaData reconnect due to {0}", ex.FormatException());
                }
            }

            return null;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (connection != null)
                {
                    lock (this)
                    {
                        if (connection != null)
                        {

                            this.connection.Dispose();
                            this.connection = null;
                        }
                    }
                }
            }
        }
    }
}
