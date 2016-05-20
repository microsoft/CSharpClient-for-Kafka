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
    using Kafka.Client.Cluster;
    using Kafka.Client.Messages;
    using log4net;
    using System.Collections.Concurrent;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using Utils;

    /// <summary>
    /// Represents topic in brokers's partition.
    /// </summary>
    public class PartitionTopicInfo
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(PartitionTopicInfo));
        private readonly object consumedOffsetLock = new object();
        private readonly object fetchedOffsetLock = new object();
        private readonly object commitedOffsetLock = new object();
        private readonly BlockingCollection<FetchedDataChunk> chunkQueue;
        private long consumedOffset;
        private long fetchedOffset;
        private long lastKnownGoodNextRequestOffset;
        private bool consumedOffsetValid = true;
        private long commitedOffset;
        private long nextRequestOffset;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionTopicInfo"/> class.
        /// </summary>
        /// <param name="topic">
        /// The topic.
        /// </param>
        /// <param name="brokerId">
        /// The broker ID.
        /// </param>
        /// <param name="partition">
        /// The broker's partition.
        /// </param>
        /// <param name="chunkQueue">
        /// The chunk queue.
        /// </param>
        /// <param name="consumedOffset">
        /// The consumed offset value.
        /// </param>
        /// <param name="fetchedOffset">
        /// The fetched offset value.
        /// </param>
        /// <param name="fetchSize">
        /// The fetch size.
        /// </param>
        public PartitionTopicInfo(
            string topic,
            int brokerId,
            int partitionId,
            BlockingCollection<FetchedDataChunk> chunkQueue,
            long consumedOffset,
            long fetchedOffset,
            int fetchSize,
            long commitedOffset)
        {
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.chunkQueue = chunkQueue;
            this.BrokerId = brokerId;
            this.consumedOffset = consumedOffset;
            this.fetchedOffset = fetchedOffset;
            this.NextRequestOffset = fetchedOffset;
            this.FetchSize = fetchSize;
            this.commitedOffset = commitedOffset;
            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat("initial CommitedOffset  of {0} is {1}", this, commitedOffset);
                Logger.DebugFormat("initial consumer offset of {0} is {1}", this, consumedOffset);
                Logger.DebugFormat("initial fetch offset of {0} is {1}", this, fetchedOffset);
            }
        }

        /// <summary>
        /// Gets broker ID.
        /// </summary>
        public int BrokerId { get; private set; }

        /// <summary>
        /// Gets the fetch size.
        /// </summary>
        public int FetchSize { get; private set; }

        /// <summary>
        /// Gets the partition Id.
        /// </summary>
        public int PartitionId { get; private set; }

        /// <summary>
        /// Gets the topic.
        /// </summary>
        public string Topic { get; private set; }

        /// <summary>
        /// Last read out from Iterator
        /// </summary>
        public long ConsumeOffset
        {
            get
            {
                lock (this.consumedOffsetLock)
                {
                    return this.consumedOffset;
                }
            }
            set
            {
                lock (this.consumedOffsetLock)
                {
                    this.consumedOffset = value;
                }

                if (Logger.IsDebugEnabled)
                {
                    Logger.DebugFormat("reset consume offset of {0} to {1}", this, value);
                }
            }
        }

        /// <summary>
        /// Last commited to zookeeper
        /// </summary>
        public long CommitedOffset
        {
            get
            {
                lock (this.commitedOffsetLock)
                {
                    return this.commitedOffset;
                }
            }
            set
            {
                lock (this.consumedOffsetLock)
                {
                    this.commitedOffset = value;
                }

                if (Logger.IsDebugEnabled)
                {
                    Logger.DebugFormat("resetcommitedOffset of {0} to {1}", this, value);
                }
            }
        }

        /// <summary>
        /// Gets or sets a flag which indicates if the current consumed offset is valid
        /// and should be used during updates to ZooKeeper.
        /// </summary>
        public bool ConsumeOffsetValid
        {
            get
            {
                return consumedOffsetValid;
            }
            set
            {
                consumedOffsetValid = value;
            }
        }

        /// <summary>
        /// Last fetched offset from Kafka
        /// </summary>
        public long FetchOffset
        {
            get
            {
                lock (this.fetchedOffsetLock)
                {
                    return this.fetchedOffset;
                }
            }

            set
            {
                lock (this.fetchedOffsetLock)
                {
                    this.fetchedOffset = value;
                    // reset the next offset as well.
                    NextRequestOffset = value;
                }

                if (Logger.IsDebugEnabled)
                {
                    Logger.DebugFormat("reset fetch offset of {0} to {1}", this, value);
                }
            }
        }

        /// <summary>
        /// It happens when it gets consumer offset out of range exception. Try to fix the offset.
        /// </summary>        
        internal void ResetOffset(long resetOffset)
        {
            lock(this.fetchedOffsetLock)
            {
                /// Save the old fetchoffset as lastKnownGoodNextRequestOffset in order to caculate how many actual messages are left in the queue
                /// Reset the fetchedoffset and nextrequestoffset to where it should be.
                this.lastKnownGoodNextRequestOffset = this.nextRequestOffset;
                this.fetchedOffset = resetOffset;
                this.nextRequestOffset = resetOffset;
            }

            if (Logger.IsDebugEnabled)
            {
                Logger.DebugFormat("Set lastKnownGoodNextRequestOffset to {0}. {1}", this.lastKnownGoodNextRequestOffset, this);
            }
        }

        internal long GetMessagesCount()
        {
            return this.lastKnownGoodNextRequestOffset - this.ConsumeOffset;
        }
        
        // Gets or sets the offset for the next request.
        public long NextRequestOffset
        {
            get
            {
                return this.nextRequestOffset;
            }
            internal set
            {
                this.nextRequestOffset = value;

                // keep lastKnownGoodNextRequestOffset in sync with nextRequestOffset in normal case
                this.lastKnownGoodNextRequestOffset = value;
            }
        }
        
        /// <summary>
        /// Ads a message set to the queue
        /// </summary>
        /// <param name="messages">The message set</param>
        /// <returns>The set size</returns>
        public int Add(BufferedMessageSet messages)
        {
            int size = messages.SetSize;
            if (size > 0)
            {
                long offset = messages.Messages.Last().Offset;

                Logger.InfoFormat("{2} : Updating fetch offset = {0} with value = {1}", this.fetchedOffset, offset, this.PartitionId);
                this.chunkQueue.Add(new FetchedDataChunk(messages, this, this.fetchedOffset));
                Interlocked.Exchange(ref this.fetchedOffset, offset);
                Logger.Debug("Updated fetch offset of " + this + " to " + offset);
            }

            return size;
        }

        public override string ToString()
        {
            return string.Format("{0}:{1}: fetched offset = {2}: consumed offset = {3}", this.Topic, this.PartitionId, this.fetchedOffset, this.consumedOffset);
        }
    }
}