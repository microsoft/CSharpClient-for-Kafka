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

namespace Kafka.Client.Producers
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers.Partitioning;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using Kafka.Client.Responses;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using Microsoft.KafkaNET.Library.Util;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Threading;

    public class DefaultCallbackHandler<TK, TV> : ICallbackHandler<TK, TV>
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger("DefaultCallbackHandler");

        private readonly ProducerConfiguration producerConfig;
        private readonly IPartitioner<TK> partitioner;
        private readonly IEncoder<TV> encoder;
        private readonly ISyncProducerPool syncProducerPool;
        private readonly IBrokerPartitionInfo brokerPartitionInfo;
        private int correlationId;
        private readonly ThreadSafeRandom random = new ThreadSafeRandom();

        public DefaultCallbackHandler(ProducerConfiguration config,
            IPartitioner<TK> partitioner,
            IEncoder<TV> encoder,
            IBrokerPartitionInfo brokerPartitionInfo,
            ISyncProducerPool syncProducerPool)
        {
            this.producerConfig = config;
            this.partitioner = partitioner;
            Logger.DebugFormat("partitioner  {0}", this.partitioner == null ? "Null" : this.partitioner.GetType().ToString());
            this.encoder = encoder;
            this.syncProducerPool = syncProducerPool;
            this.brokerPartitionInfo = brokerPartitionInfo;
        }

        private int NextCorrelationId
        {
            get { return Interlocked.Increment(ref correlationId); }
        }

        public void Handle(IEnumerable<ProducerData<TK, TV>> events)
        {
            IEnumerable<ProducerData<TK, Message>> serializedData = this.Serialize(events);
            ProduceDispatchSeralizeResult<TK> outstandingProduceRequests = new ProduceDispatchSeralizeResult<TK>(new List<Exception> { }, serializedData, null, true);
            var remainingRetries = this.producerConfig.ProducerRetries;
            int currentRetryMs = producerConfig.ProducerRetryExponentialBackoffMinMs;

            var brokers = this.producerConfig.Brokers;
            if (producerConfig.Verbose)
            {
                Logger.DebugFormat("Handle,producerConfig.Brokers.Count={0},broker[0]={1}", brokers.Count, brokers.Any() ? brokers[0].ToString() : "NO broker");
            }

            while (remainingRetries > 0 && outstandingProduceRequests.HasDataNeedDispatch)
            {
                try
                {
                    ProduceDispatchSeralizeResult<TK> currentOutstandingRequests =
                        this.DispatchSerializedData(outstandingProduceRequests.FailedProducerDatas, remainingRetries > 1 ? false : true);
                    outstandingProduceRequests = currentOutstandingRequests;
                    if (outstandingProduceRequests.HasDataNeedDispatch)
                    {
                        currentRetryMs = ExponentialRetry(currentRetryMs);
                        remainingRetries--;
                    }
                    else
                    {
                        currentRetryMs = producerConfig.ProducerRetryExponentialBackoffMinMs;
                        break;
                    }
                }
                catch (Exception e)
                {
                    remainingRetries--;
                    if (remainingRetries > 0)
                    {
                        continue;
                    }
                    int allCount = events.Count();
                    int remainFailedCount = outstandingProduceRequests.FailedProducerDatas.ToList().Count;
                    string message = FailedToSendMessageException<TK>.BuildExceptionMessage(new List<Exception>() { e }, producerConfig.ProducerRetries, allCount, remainFailedCount, outstandingProduceRequests);
                    Logger.Error(message);
                    throw new FailedToSendMessageException<TK>(message, new List<Exception>() { e }, outstandingProduceRequests, allCount, remainFailedCount);
                }
            }

            if (outstandingProduceRequests.HasDataNeedDispatch)
            {
                int allCount = events.Count();
                int remainFailedCount = outstandingProduceRequests.FailedProducerDatas.ToList().Count;
                string message = FailedToSendMessageException<TK>.BuildExceptionMessage(new List<Exception>(), producerConfig.ProducerRetries, allCount, remainFailedCount, outstandingProduceRequests);
                Logger.Error(message);
                throw new FailedToSendMessageException<TK>(message, new List<Exception>(), outstandingProduceRequests, allCount, remainFailedCount);
            }
        }

        private int ExponentialRetry(int currentRetryMs)
        {
            Thread.Sleep(currentRetryMs);
            return Math.Min(producerConfig.ProducerRetryExponentialBackoffMaxMs, currentRetryMs * 2);
        }

        private IEnumerable<ProducerData<TK, Message>> Serialize(IEnumerable<ProducerData<TK, TV>> events)
        {
            return events.Select(
                e => new ProducerData<TK, Message>(e.Topic, e.Key, e.IsKeyNull, e.Data.Select(m => this.encoder.ToMessage(m))));
        }

        private ProduceDispatchSeralizeResult<TK> DispatchSerializedData(IEnumerable<ProducerData<TK, Message>> messages, bool lastRetry)
        {
            List<ProducerData<TK, Message>> failedProduceRequests = null;
            List<Tuple<int, TopicAndPartition, ProducerResponseStatus>> failedDetail = null;
            var exceptions = new List<Exception>();
            bool hasDataNeedReprocess = false;
            try
            {
                IEnumerable<KeyValuePair<int, Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>>>> partitionedData = this.PartitionAndCollate(messages);
                foreach (KeyValuePair<int, Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>>> keyValuePair in partitionedData)
                {
                    int brokerId = keyValuePair.Key;
                    Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>> eventsPerBrokerMap = keyValuePair.Value;
                    Dictionary<TopicAndPartition, BufferedMessageSet> messageSetPerBroker = this.GroupMessagesToSet(eventsPerBrokerMap);
                    if (producerConfig.Verbose)
                    {
                        Logger.DebugFormat("ProducerDispatchSeralizeResult,brokerId={0},partitionData.Count={1}", brokerId, partitionedData.Count());
                    }

                    ProducerSendResult<IEnumerable<Tuple<TopicAndPartition, ProducerResponseStatus>>> failedTopicResponse = this.Send(brokerId, messageSetPerBroker);
                    if (!failedTopicResponse.Success || (failedTopicResponse.ReturnVal != null && failedTopicResponse.ReturnVal.Any()))
                    {
                        failedProduceRequests = new List<ProducerData<TK, Message>>();
                        foreach (var failedTopic in failedTopicResponse.ReturnVal)
                        {
                            List<ProducerData<TK, Message>> failedMessages = eventsPerBrokerMap[failedTopic.Item1];
                            failedProduceRequests.AddRange(failedMessages);
                            hasDataNeedReprocess = true;
                        }

                        foreach (var topic in failedTopicResponse.ReturnVal.Select(e => e.Item1.Topic).Distinct())
                        {
                            // update the metadata in case that the failure caused by kafka broker failover
                            this.brokerPartitionInfo.UpdateInfo(producerConfig.VersionId, NextCorrelationId,
                                producerConfig.ClientId, topic);
                        }

                        if (lastRetry)
                        {
                            failedDetail = new List<Tuple<int, TopicAndPartition, ProducerResponseStatus>>();
                            foreach (var failedTopic in failedTopicResponse.ReturnVal)
                            {
                                failedDetail.Add(new Tuple<int, TopicAndPartition, ProducerResponseStatus>(brokerId, failedTopic.Item1, failedTopic.Item2));
                            }
                        }
                    }
                    if (failedTopicResponse.Exception != null)
                        exceptions.Add(failedTopicResponse.Exception);


                }
            }
            catch (Exception)
            {
                //Will be catch and log in Handle, so do nothing here
                throw;
            }

            return new ProduceDispatchSeralizeResult<TK>(exceptions, failedProduceRequests, failedDetail, hasDataNeedReprocess);
        }
        /// <summary>
        /// Send message of one broker.
        /// </summary>
        /// <param name="brokerId"></param>
        /// <param name="messagesPerTopic"></param>
        /// <returns></returns>
        private ProducerSendResult<IEnumerable<Tuple<TopicAndPartition, ProducerResponseStatus>>> Send(int brokerId, IDictionary<TopicAndPartition, BufferedMessageSet> messagesPerTopic)
        {
            try
            {
                if (brokerId < 0)
                {
                    throw new NoLeaderForPartitionException(
                        string.Format("No leader for some partition(s).  And it try write to on invalid broker {0}.  The assigned TopicAndPartition for the data is :{1} ", brokerId, messagesPerTopic.Any() ? messagesPerTopic.First().Key.ToString() : "(null)"));
                }
                if (messagesPerTopic.Any())
                {
                    var producerRequest = new ProducerRequest(NextCorrelationId,
                                                              this.producerConfig.ClientId,
                                                              this.producerConfig.RequiredAcks,
                                                              this.producerConfig.AckTimeout,
                                                              messagesPerTopic);
                    ISyncProducer syncProducer = null;
                    try
                    {
                        syncProducer = this.syncProducerPool.GetProducer(brokerId);
                    }
                    catch (UnavailableProducerException e)
                    {
                        Logger.Error(e.Message);
                        // When initializing producer pool, some broker might be unavailable, and now it is healthy and is leader for some partitions.
                        // A new producer should be added to the pool, creating a TCP connection to the broker.
                        var broker =
                            this.brokerPartitionInfo.GetBrokerPartitionLeaders(messagesPerTopic.Keys.First().Topic)
                                .Values.FirstOrDefault(b => b.Id == brokerId);
                        if (broker != null)
                        {
                            this.syncProducerPool.AddProducer(broker);
                            syncProducer = this.syncProducerPool.GetProducer(brokerId);
                        }
                    }

                    if (producerConfig.Verbose)
                    {
                        Logger.DebugFormat("Kafka producer before sent messages for topics {0} to broker {1}", messagesPerTopic, brokerId);
                    }
                    ProducerResponse response = syncProducer.Send(producerRequest);
                    if (this.producerConfig.Verbose)
                    {
                        string msg = string.Format("Kafka producer sent messages for topics {0} to broker {1} on {2}:{3}",
                                           messagesPerTopic, brokerId, syncProducer.Config.Host, syncProducer.Config.Port);
                        Logger.Debug(msg);
                    }

                    if (response != null)
                    {
                        int statusCount = response.Statuses.Count;
                        //In java version
                        //https://git-wip-us.apache.org/repos/asf?p=kafka.git;a=blob;f=core/src/main/scala/kafka/producer/async/DefaultEventHandler.scala;h=821901e4f434dfd9eec6eceabfc2e1e65507a57c;hb=HEAD#l260
                        //The producerRequest.data just the messagesPerTopic.  So there compare the statusCount with producerRequest.data.size
                        //But in this C# version, the producerRequest.Data already grouped by topic.  So here need compare with messagesPerTopic.Count()
                        int requestCount = messagesPerTopic.Count;
                        if (statusCount != requestCount)
                        {
                            StringBuilder sb = new StringBuilder();
                            sb.AppendFormat("Incomplete response count {0} for producer request count {1}. ", statusCount, requestCount);
                            sb.AppendFormat(" Broker {0} on {1}:{2}", brokerId, syncProducer.Config.Host, syncProducer.Config.Port);
                            sb.Append(" Message detail:");
                            sb.Append(string.Join(",", messagesPerTopic.Select(r => string.Format("{0},{1}", r.Key.Topic, r.Key.PartitionId))));
                            sb.Append(" Response status detail which has error:");
                            sb.Append(string.Join(",", response.Statuses.Where(r => r.Value.Error != (short)ErrorMapping.NoError).Select(r => r.ToString())));
                            throw new FailedToSendMessageException<TK>(sb.ToString());
                        }
                        return new ProducerSendResult<IEnumerable<Tuple<TopicAndPartition, ProducerResponseStatus>>>(response.Statuses.Where(s => s.Value.Error != (short)ErrorMapping.NoError)
                            .Select(s => new Tuple<TopicAndPartition, ProducerResponseStatus>(s.Key, s.Value)));
                    }
                }
            }
            catch (NoLeaderForPartitionException e)
            {
                Logger.Error(ExceptionUtil.GetExceptionDetailInfo(e));
                return new ProducerSendResult<IEnumerable<Tuple<TopicAndPartition, ProducerResponseStatus>>>(messagesPerTopic.Keys.Select(
                    s => new Tuple<TopicAndPartition, ProducerResponseStatus>(s, new ProducerResponseStatus { Error = ErrorMapping.NotLeaderForPartitionCode })), e);
            }
            catch (Exception e)
            {
                Logger.Error(ExceptionUtil.GetExceptionDetailInfo(e));
                return new ProducerSendResult<IEnumerable<Tuple<TopicAndPartition, ProducerResponseStatus>>>(messagesPerTopic.Keys.Select(
                    s => new Tuple<TopicAndPartition, ProducerResponseStatus>(s, new ProducerResponseStatus { Error = ErrorMapping.UnknownCode })), e);
            }

            return new ProducerSendResult<IEnumerable<Tuple<TopicAndPartition, ProducerResponseStatus>>>(Enumerable.Empty<Tuple<TopicAndPartition, ProducerResponseStatus>>());
        }

        /// <summary>
        /// Given the message to be pushed, return the partition selected, the broker leader for the partition
        /// </summary>
        /// <param name="events">message set to be produced</param>
        /// <returns>the partition selected and the broker leader</returns>
        private IEnumerable<KeyValuePair<int, Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>>>> PartitionAndCollate(IEnumerable<ProducerData<TK, Message>> events)
        {
            var ret = new Dictionary<int, Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>>>();

            if (this.producerConfig.ForceToPartition >= 0)
            {
                int leaderBrokerId = this.producerConfig.Brokers[0].BrokerId;
                Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>> dataPerBroker = new Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>>();
                dataPerBroker.Add(new TopicAndPartition(events.First().Topic, this.producerConfig.ForceToPartition), events.ToList());
                if (producerConfig.Verbose)
                {
                    Logger.DebugFormat("PartitionAndCollate ForceToPartition ,totalNumPartitions={0},ForceToPartition={1},leaderBrokerId={2}",
                        0, this.producerConfig.ForceToPartition, leaderBrokerId);
                }
                ret.Add(leaderBrokerId, dataPerBroker);
            }
            else
            {
                foreach (var eventItem in events)
                {
                    //Sorted list of all partition,  some has leader, some not.
                    List<Partition> topicPartitionsList = this.GetPartitionListForTopic(eventItem);

                    // when the total number of partitions is specified in the ProducerConf, do not check for the number of active partitions again,
                    // this ensures the partition selected per the pre-determined number of partitions, instead of actually number of partitions checked at run-time
                    var totalNumPartitions = this.producerConfig.TotalNumPartitions == 0 ? topicPartitionsList.Count : this.producerConfig.TotalNumPartitions;
                    var partitionIndex = this.GetPartition(eventItem.Key, eventItem.IsKeyNull, totalNumPartitions);

                    // when the total number of partition is specified, this.GetPartitionListForTopic() returns only one partition corresponding to the partitionIndex
                    Partition brokerPartition = this.producerConfig.TotalNumPartitions == 0 ? topicPartitionsList.ElementAt(partitionIndex) : topicPartitionsList[0];
                    var leaderBrokerId = brokerPartition.Leader != null ? brokerPartition.Leader.BrokerId : -1; // postpone the failure until the send operation, so that requests for other brokers are handled correctly
                    if (producerConfig.Verbose)
                    {
                        Logger.DebugFormat("PartitionAndCollate,totalNumPartitions={0},eventItem.Key={1},partitionIndex={2},brokerPartition={3},leaderBrokerId={4}",
                        totalNumPartitions, eventItem.Key, partitionIndex, brokerPartition, leaderBrokerId);
                    }

                    if (leaderBrokerId == -1)
                    {
                        Logger.WarnFormat("No leader for partition {0} brokerPartition:{1} ", partitionIndex, brokerPartition.ToString());
                    }

                    Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>> dataPerBroker = null;
                    if (ret.ContainsKey(leaderBrokerId))
                    {
                        dataPerBroker = ret[leaderBrokerId];
                    }
                    else
                    {
                        dataPerBroker = new Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>>();
                        ret.Add(leaderBrokerId, dataPerBroker);
                    }

                    var topicAndPartition = new TopicAndPartition(eventItem.Topic, brokerPartition.PartId);
                    List<ProducerData<TK, Message>> dataPerTopicPartition = null;
                    if (dataPerBroker.ContainsKey(topicAndPartition))
                    {
                        dataPerTopicPartition = dataPerBroker[topicAndPartition];
                    }
                    else
                    {
                        dataPerTopicPartition = new List<ProducerData<TK, Message>>();
                        dataPerBroker.Add(topicAndPartition, dataPerTopicPartition);
                    }
                    dataPerTopicPartition.Add(eventItem);
                }
            }
            return ret;
        }

        /// <summary>
        /// When TotalNumPartitions is not specified in ProducerConf, it retrieves a set of partitions from the broker; 
        /// otherwise, returns only the partition determined by ProducerData.Key
        /// </summary>
        /// <param name="pd">ProducerData to be produced</param>
        /// <returns>a list of partitions for the target topic</returns>
        private List<Partition> GetPartitionListForTopic(ProducerData<TK, Message> pd)
        {
            List<Partition> topicPartitionsList = null;

            if (this.producerConfig.ForceToPartition >= 0)
            {
                int brokerLeaderId = this.producerConfig.Brokers[0].BrokerId;
                topicPartitionsList = new List<Partition>() {
                    new Partition(pd.Topic,this.producerConfig.ForceToPartition) { Leader = new Replica(brokerLeaderId, pd.Topic) } };
            }
            else if (this.producerConfig.TotalNumPartitions == 0)
            {
                topicPartitionsList = this.brokerPartitionInfo.GetBrokerPartitionInfo(producerConfig.VersionId,
                    producerConfig.ClientId, NextCorrelationId, pd.Topic);
            }
            else
            {
                //TODO:  here the     brokerLeaderId  maybe not Leader  of the calculated partition. Totally mess up.   
                // returns THE partition determiend by ProducerData.Key
                int brokerLeaderId = this.producerConfig.Brokers.Any() ? this.producerConfig.Brokers[0].BrokerId : -1;
                topicPartitionsList = new List<Partition>() {
                    new Partition(pd.Topic, this.GetPartition(pd.Key, pd.IsKeyNull, this.producerConfig.TotalNumPartitions)) { Leader = new Replica(brokerLeaderId, pd.Topic) } };
            }
            if (this.producerConfig.Verbose)
            {
                Logger.DebugFormat("GetPartitionListForTopic,Broker partitions registered for topic: {0} are {1}", pd.Topic, string.Join(",", topicPartitionsList.Select(p => p.PartId.ToString(CultureInfo.InvariantCulture))));
            }

            if (!topicPartitionsList.Any())
            {
                throw new NoBrokersForPartitionException("Partition = " + pd.Key);
            }
            return topicPartitionsList;
        }

        private int GetPartition(TK key, bool isKeyNull, int numPartitions)
        {
            if (numPartitions <= 0)
            {
                throw new InvalidPartitionException(
                    string.Format("Invalid number of partitions: {0}. Valid values are > 0", numPartitions));
            }

            //TODO: In java version, if key is null, will cache one partition for the topic.
            var partition = (key == null || isKeyNull) ? random.Next(numPartitions) : this.partitioner.Partition(key, numPartitions);
            if (partition < 0 || partition >= numPartitions)
            {
                throw new InvalidPartitionException(
                    string.Format("Invalid partition id : {0}. Valid values are in the range inclusive [0, {1}]",
                                  partition, (numPartitions - 1)));
            }
            return partition;
        }

        private Dictionary<TopicAndPartition, BufferedMessageSet> GroupMessagesToSet(Dictionary<TopicAndPartition, List<ProducerData<TK, Message>>> eventsPerTopicAndPartition)
        {
            Dictionary<TopicAndPartition, BufferedMessageSet> messagesPerTopicPartition = new Dictionary<TopicAndPartition, BufferedMessageSet>();
            foreach (KeyValuePair<TopicAndPartition, List<ProducerData<TK, Message>>> keyValuePair in eventsPerTopicAndPartition)
            {
                TopicAndPartition topicAndPartition = keyValuePair.Key;
                List<ProducerData<TK, Message>> produceData = keyValuePair.Value;
                List<Message> messages = new List<Message>();
                produceData.ForEach(p => messages.AddRange(p.Data));
                switch (this.producerConfig.CompressionCodec)
                {
                    case CompressionCodecs.NoCompressionCodec:
                        messagesPerTopicPartition.Add(topicAndPartition,
                            new BufferedMessageSet(CompressionCodecs.NoCompressionCodec, messages, topicAndPartition.PartitionId));
                        break;
                    default:
                        byte magic = 0;
                        byte attributes = 0;
                        foreach (Message m in messages)
                        {
                            magic = m.Magic;
                            attributes = m.Attributes;
                            m.CleanMagicAndAttributesBeforeCompress();
                        }
                        if (!this.producerConfig.CompressedTopics.Any() || this.producerConfig.CompressedTopics.Contains(topicAndPartition.Topic))
                        {
                            messagesPerTopicPartition.Add(topicAndPartition, new BufferedMessageSet(this.producerConfig.CompressionCodec, messages, topicAndPartition.PartitionId));
                        }
                        else
                        {
                            messagesPerTopicPartition.Add(topicAndPartition, new BufferedMessageSet(CompressionCodecs.NoCompressionCodec, messages, topicAndPartition.PartitionId));
                        }
                        foreach (Message m in messages)
                        {
                            m.RestoreMagicAndAttributesAfterCompress(magic, attributes);
                        }
                        break;
                }
            }

            return messagesPerTopicPartition;
        }

        public void Dispose()
        {
            if (this.syncProducerPool != null)
            {
                this.syncProducerPool.Dispose();
            }
        }
    }

    public class FailedToSendMessageException<K> : Exception
    {
        public int CountAll { get; private set; }
        public int CountFailed { get; private set; }

        public FailedToSendMessageException(string s)
            : base(s)
        {
        }

        public FailedToSendMessageException(string s, List<Exception> exceptions, ProduceDispatchSeralizeResult<K> outstandingProduceRequests, int all, int failed)
            : base(s)
        {
            this.ProduceDispatchSeralizeResult = outstandingProduceRequests;
            this.LastProduceExceptions = exceptions;
            this.CountAll = all;
            this.CountFailed = failed;
        }

        public ProduceDispatchSeralizeResult<K> ProduceDispatchSeralizeResult { get; private set; }

        public List<Exception> LastProduceExceptions { get; private set; }

        public static string BuildExceptionMessage(string message, List<Exception> exceptions)
        {
            var builder = new StringBuilder();
            int count = 0;
            foreach (var e in exceptions)
            {
                builder.AppendFormat("{0} th:", count);
                builder.AppendFormat("{0} {1} {2} \r\n", e.Message, e.StackTrace, e.Source);
                count++;
            }

            return message + " All internal Exceptions(" + count.ToString() + ") : " + builder.ToString();
        }

        internal static string BuildExceptionMessage(List<Exception> exceptions, int retry, int allCount, int remainFailedCount, ProduceDispatchSeralizeResult<K> outstandingProduceRequests)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("Failed to send messages after {0} tries. FailedProducerDatas not empty. Success Count:{1} Failed Count: {2}.", retry, allCount - remainFailedCount, remainFailedCount);

            if (exceptions != null && exceptions.Any())
            {
                StringBuilder builder = new StringBuilder();
                int count = 0;
                foreach (var e in exceptions)
                {
                    builder.AppendFormat("{0} th:", count);
                    builder.AppendFormat("{0} {1} {2} \r\n", e.Message, e.StackTrace, e.Source);
                    count++;
                }
                sb.AppendFormat("\r\n================Internal exceptions: {0}   {1} ", count, builder.ToString());
            }

            if (outstandingProduceRequests.FailedProducerDatas != null)
            {
                sb.Append("\r\n================Failed sent message key: ");
                sb.Append(string.Join(",", outstandingProduceRequests.FailedProducerDatas.Select(r => string.Format("Topic: {0} Key: {1}"
                    , r.Topic
                    , r.Key))));
            }

            if (outstandingProduceRequests.FailedDetail != null)
            {
                sb.Append("\r\n================Failed Detail: ");
                sb.Append(string.Join(",", outstandingProduceRequests.FailedDetail.Select(r => string.Format("Broker:{0},{1},{2} \t", r.Item1, r.Item2, r.Item3))));
            }

            if (outstandingProduceRequests.Exceptions != null && outstandingProduceRequests.Exceptions.Any())
            {
                StringBuilder builder = new StringBuilder();
                int count = 0;
                foreach (var e in outstandingProduceRequests.Exceptions)
                {
                    builder.AppendFormat("{0} th:", count);
                    builder.AppendFormat("{0} {1} {2} \r\n", e.Message, e.StackTrace, e.Source);
                    count++;
                }
                sb.AppendFormat("\r\n================ProduceDispatchSeralizeResult Internal exceptions: {0}   {1} ", count, builder.ToString());
            }

            return sb.ToString();
        }
    }
}
