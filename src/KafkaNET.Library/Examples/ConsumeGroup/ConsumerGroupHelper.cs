// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client;
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Exceptions;
    using Kafka.Client.Messages;
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Examples of using ConsumerGroup.
    /// Single thread or multiple threads.
    /// </summary>
    internal class ConsumerGroupHelper
    {
        public static Dictionary<int, long> initialOffset = new Dictionary<int, long>();
        public static Dictionary<int, long> newOffset = new Dictionary<int, long>();

        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumerGroupHelper));
        internal static long totalCount = 0;

        internal static void DumpMessageAsConsumerGroup(ConsumeGroupHelperOptions cgOptions)
        {
            try
            {
                totalCount = 0;

                ZookeeperConsumerConnector.UseSharedStaticZookeeperClient = cgOptions.UseSharedStaticZookeeperClient;
                DumpMessageAsConsumerGroupSigleThreadBlock(cgOptions);

                Logger.InfoFormat("======TotallyRead:{0}=============", Interlocked.Read(ref totalCount));
                Console.WriteLine("======TotallyRead:{0}=============", Interlocked.Read(ref totalCount));

                Logger.InfoFormat("======New offset");
                Console.WriteLine("======New offset");
                long totalCountCommit = 0;
                foreach (var kv in newOffset.OrderBy(r => r.Key))
                {
                    string d = string.Format("Partition:{0}\t Old Offset:{1,10} --> New Offset:{2,10}  Diff:{3}"
                        , kv.Key, initialOffset == null || !initialOffset.ContainsKey(kv.Key) ? 0 : initialOffset[kv.Key],
                        kv.Value,
                        kv.Value - (initialOffset == null || !initialOffset.ContainsKey(kv.Key) ? 0 : initialOffset[kv.Key]));
                    Logger.Info(d);
                    Console.WriteLine(d);
                    totalCountCommit += kv.Value - (initialOffset == null || !initialOffset.ContainsKey(kv.Key) ? 0 : initialOffset[kv.Key]);
                }
                //TODO: currently each partition maybe missed one message hasn't been commit.  please refer kafka document:https://cwiki.apache.org/confluence/display/KAFKA/Compression
                //Hence, for compressed data, the consumed offset will be advanced one compressed message at a time. This has the side effect of possible duplicates in the event of a consumer failure. For uncompressed data, consumed offset will be advanced one message at a time.
                if (totalCountCommit != Interlocked.Read(ref totalCount))
                {
                    Logger.ErrorFormat("totalCountCommit {0} !=  totalCount  {1}, check next line log see if it's reasonable:", totalCountCommit, Interlocked.Read(ref totalCount));
                    long diff = totalCountCommit - Interlocked.Read(ref totalCount);
                    if (diff <= newOffset.Count && diff >= 0)
                    {
                        Logger.ErrorFormat(" the difference is reasonable, by design of kafkaNET.Library.   For each partition, if not hit end of log, at least read one record from it!");
                    }
                    else
                    {
                        Logger.ErrorFormat(" the difference is not reasonable ,please check log!");
                    }
                }
                else
                {
                    Logger.InfoFormat("totalCountCommit {0} ==  totalCount  {1}", totalCountCommit, Interlocked.Read(ref totalCount));
                    Console.WriteLine("totalCountCommit {0} ==  totalCount  {1}", totalCountCommit, Interlocked.Read(ref totalCount));
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Consumer group consume data. got exception:{0}\r\ninput parameter:Topic:{1}\tZookeeper:{2}\tconsumerGroupName:{3}\tconsumerId:{4}\tthreadCount:{5}\tcount:{6}"
                     , ex.FormatException(),
                        cgOptions.Topic,
                        cgOptions.Zookeeper,
                        cgOptions.ConsumerGroupName,
                        cgOptions.ConsumerId,
                        cgOptions.FetchThreadCountPerConsumer,
                        cgOptions.Count);
            }
        }
        //#region Option 1, single ThreadBlock
        private static void DumpMessageAsConsumerGroupSigleThreadBlock(ConsumeGroupHelperOptions cgOptions)
        {

            initialOffset = null;
            newOffset = null;
            AutoResetEvent[] autos = new AutoResetEvent[cgOptions.ZookeeperConnectorCount];

            for (int i = 0; i < cgOptions.ZookeeperConnectorCount; i++)
            {
                AutoResetEvent resetEvent = new AutoResetEvent(false);
                ConsumerGroupHelperUnit unit = new ConsumerGroupHelperUnit(i, cgOptions, resetEvent, cgOptions.ZookeeperConnectorConsumeMessageCount[i]);
                Thread t = new Thread(unit.Consume);
                t.Start();
                Logger.InfoFormat("Start thread {0} of ZookeeperConsumerConnector", i);
                autos[i] = resetEvent;
            }
            WaitHandle.WaitAll(autos);
        }

    }

    internal class ConsumerGroupHelperUnit
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumerGroupHelperUnit));
        internal int ThreadID;
        internal ConsumerConfiguration configSettings;
        ConsumeGroupHelperOptions cgOptions;
        AutoResetEvent resetEvent;
        internal int Count = -1;
        internal int consumedTotalCount = 0;
        internal ConsumerGroupHelperUnit(int threadID, ConsumeGroupHelperOptions cg, AutoResetEvent e, int c)
        {
            this.resetEvent = e;
            this.cgOptions = cg;
            this.ThreadID = threadID;
            this.Count = c;
            configSettings = new ConsumerConfiguration
            {
                AutoOffsetReset = OffsetRequest.SmallestTime,
                AutoCommit = false,
                GroupId = cgOptions.ConsumerGroupName,
                ConsumerId = cgOptions.ConsumerId + "_Thread_" + threadID.ToString(),
                Timeout = cgOptions.Timeout,
                ZooKeeper = new ZooKeeperConfiguration(cgOptions.Zookeeper, 30000, 4000, 8000),
                BufferSize = cgOptions.BufferSize,
                FetchSize = cgOptions.FetchSize,
                MaxFetchBufferLength = cgOptions.MaxFetchBufferLength// cgOptions.FetchSize * (10~40) / cgOptions.MessageSize,  
            };
            if (cgOptions.CancellationTimeoutMs != KafkaNETExampleConstants.DefaultCancellationTimeoutMs)
                configSettings.Timeout = -1;
        }
        internal void Consume()
        {
            // connects to zookeeper 
            using (ZookeeperConsumerConnector connector = new ZookeeperConsumerConnector(configSettings, true))
            {
                if (this.ThreadID == 0)
                {
                    ConsumerGroupHelper.initialOffset = connector.GetOffset(cgOptions.Topic);

                    Logger.InfoFormat("======Original offset \r\n{0}", ConsumerGroupHelper.initialOffset == null ? "(NULL)" : ConsumeGroupMonitorHelper.GetComsumerGroupOffsetsAsLog(ConsumerGroupHelper.initialOffset));
                }

                // defines collection of topics and number of threads to consume it with
                // ===============NOTE============================
                // For example , if there is 80 partitions for one topic.
                // 
                // Normally start more than 96 = 80*120% clients with same GroupId.  ( the extra 20% are buffer for autopilot IMP).  And set  FetchThreadCountPerConsumer as 1.
                // Then 80 clients can lock partitions,  can set MACHINENAME_ProcessID as ConsumerId, other 16 are idle.  Strongly recomand take this method.
                // 
                // If start 40 clients,  and  set  FetchThreadCountPerConsumer as 1. then every client can lock 2 partitions at least.   And if some client not available for autopilot
                // IMP reason, then some of the client maybe lock 3 partitions.   
                //
                //  If start 40 clients, and set  FetchThreadCountPerConsumer as 2,  you will get two IEnumerator<Message>:topicData[0].GetEnumerator(),topicData[1].GetEnumerator()
                //  you need start TWO threads to process them in dependently.    
                //  If the client get 2 partitions, each thread will handle 1 partition, 
                //  If the client get 3 partitions, then one thread get 2 partitions, the other one get 1 partition.  It will make the situaiton complex and the consume of partition not balance.
                //==================NOTE=============================
                IDictionary<string, int> topicMap = new Dictionary<string, int> { { cgOptions.Topic, cgOptions.FetchThreadCountPerConsumer } };

                // get references to topic streams.
                IDictionary<string, IList<KafkaMessageStream<Message>>> streams = connector.CreateMessageStreams(topicMap, new DefaultDecoder());
                IList<KafkaMessageStream<Message>> topicData = streams[cgOptions.Topic];
                long latestTotalCount = 0;

                bool hitEndAndCommited = false;
                if (cgOptions.CancellationTimeoutMs == KafkaNETExampleConstants.DefaultCancellationTimeoutMs)
                {
                    // Get the message enumerator.
                    IEnumerator<Message> messageEnumerator = topicData[0].GetEnumerator();
                    //TODO:  the enumerator count equal with FetchThreadCountPerConsumer . For example,  if that value is 5, then here should get 5 enumerator.
                    //IF have 100 partitions, and only 20 consumers, need set this value to 5.  and start 5 thread handle each one.

                    // Add tuples until maximum receive message count is reached or no new messages read after consumer configured timeout.
                    while (true)
                    {
                        bool noMoreMessage = false;
                        try
                        {
                            messageEnumerator.MoveNext();
                            Message m = messageEnumerator.Current;
                            latestTotalCount = Interlocked.Increment(ref ConsumerGroupHelper.totalCount);
                            Logger.InfoFormat("Message {0} from Partition:{1}, Offset:{2}, key:{3}, value:{4}", latestTotalCount, m.PartitionId, m.Offset, m.Key == null ? "(null)" : Encoding.UTF8.GetString(m.Key), m.Payload == null ? "(null)" : Encoding.UTF8.GetString(m.Payload));
                            if (latestTotalCount == 1)
                            {
                                Logger.InfoFormat("Read FIRST message, it's offset: {0}  PartitionID:{1}", m.Offset, ((ConsumerIterator<Message>)messageEnumerator).currentTopicInfo.PartitionId);
                            }

                            hitEndAndCommited = false;
                            if (latestTotalCount % cgOptions.CommitBatchSize == 0)
                            {
                                //NOTE======
                                //Normally, just directly call .CommitOffsets()
                                //    CommitOffset(string topic, int partition, long offset)  only used when customer has strong requirement for reprocess messages as few as possible.
                                //Need tune the frequecy of calling  .CommitOffsets(), it will directly increate zookeeper load and impact your overall performance
                                if (cgOptions.CommitOffsetWithPartitionIDOffset)
                                    connector.CommitOffset(cgOptions.Topic, m.PartitionId.Value, m.Offset);
                                else
                                    connector.CommitOffsets();
                                Console.WriteLine("\tRead some and commit once,  LATEST message offset: {0}. PartitionID:{1} -- {2}  Totally read  {3}  will commit offset. {4} FetchOffset:{5}  ConsumeOffset:{6} CommitedOffset:{7}"
                                    , m.Offset, m.PartitionId.Value, ((ConsumerIterator<Message>)messageEnumerator).currentTopicInfo.PartitionId, latestTotalCount, DateTime.Now
                                    , ((ConsumerIterator<Message>)messageEnumerator).currentTopicInfo.FetchOffset
                                    , ((ConsumerIterator<Message>)messageEnumerator).currentTopicInfo.ConsumeOffset
                                    , ((ConsumerIterator<Message>)messageEnumerator).currentTopicInfo.CommitedOffset);
                            }

                            if (cgOptions.Count > 0 && latestTotalCount >= cgOptions.Count)
                            {
                                Logger.InfoFormat("Read LAST message, it's offset: {0}. PartitionID:{1}   Totally read {2}  want {3} will exit.", m.Offset, ((ConsumerIterator<Message>)messageEnumerator).currentTopicInfo.PartitionId, latestTotalCount, cgOptions.Count);
                                break;
                            }
                        }
                        catch (ConsumerTimeoutException)
                        {
                            if (!hitEndAndCommited)
                            {
                                Logger.InfoFormat("Totally Read {0}  will commit offset. {1}", latestTotalCount, DateTime.Now);
                                connector.CommitOffsets();
                                hitEndAndCommited = true;
                            }
                            // Thrown if no new messages read after consumer configured timeout.
                            noMoreMessage = true;
                        }

                        if (noMoreMessage)
                        {
                            Logger.InfoFormat("No more message , hit end ,will Sleep(1), {0}", DateTime.Now);
                            if (cgOptions.SleepTypeWhileAlwaysRead == 0)
                                Thread.Sleep(0);
                            else if (cgOptions.SleepTypeWhileAlwaysRead == 1)
                                Thread.Sleep(1);        //Best choice is Thread.Sleep(1).  Other 3 choice still make the CPU 100%
                            else if (cgOptions.SleepTypeWhileAlwaysRead == 2)
                                Thread.Yield();
                            else
                            {

                            }
                        }
                    }
                }
                else
                {
                    //Siphon scenario, repeatly take some messages and process. if no enough messages, will stop current batch after timeout.
                    while (true)
                    {
#if NET45
                        bool noMoreMessage = false;
                        Message lastMessage = null;
                        int count = 0;
                        KafkaMessageStream<Message> messagesStream = null;
                        ConsumerIterator<Message> iterator = null;
                        using (CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(cgOptions.CancellationTimeoutMs))
                        {
                            lastMessage = null;
                            IEnumerable<Message> messages = topicData[0].GetCancellable(cancellationTokenSource.Token);
                            messagesStream = (KafkaMessageStream<Message>)messages;
                            iterator = (ConsumerIterator<Message>)messagesStream.iterator;
                            foreach (Message message in messages)
                            {
                                latestTotalCount = Interlocked.Increment(ref ConsumerGroupHelper.totalCount);
                                lastMessage = message;
                                if (latestTotalCount == 1)
                                {
                                    PartitionTopicInfo p = iterator.currentTopicInfo;
                                    Logger.InfoFormat("Read FIRST message, it's offset: {0}  PartitionID:{1}", lastMessage.Offset, p == null ? "null" : p.PartitionId.ToString());
                                }
                                hitEndAndCommited = false;
                                if (++count >= cgOptions.CommitBatchSize)
                                {
                                    cancellationTokenSource.Cancel();
                                }
                            }
                        }
                        if (count > 0)
                        {
                            connector.CommitOffsets();
                            consumedTotalCount += count;
                            PartitionTopicInfo p = iterator.currentTopicInfo;
                            Console.WriteLine("\tRead some and commit once, Thread: {8}  consumedTotalCount:{9} Target:{10} LATEST message offset: {0}. PartitionID:{1} -- {2}  Totally read  {3}  will commit offset. {4} FetchOffset:{5}  ConsumeOffset:{6} CommitedOffset:{7}"
                                    , lastMessage.Offset, lastMessage.PartitionId.Value, p == null ? "null" : p.PartitionId.ToString(), latestTotalCount, DateTime.Now
                                    , p == null ? "null" : p.FetchOffset.ToString()
                                    , p == null ? "null" : p.ConsumeOffset.ToString()
                                    , p == null ? "null" : p.CommitedOffset.ToString()
                                    , this.ThreadID
                                    , this.consumedTotalCount
                                    , this.Count);
                        }
                        else
                        {
                            noMoreMessage = true;
                        }

                        if (this.Count > 0 && consumedTotalCount >= this.Count)
                        {
                            Logger.InfoFormat("Current thrad Read LAST message, Totally read {0}  want {1} will exit current thread.", consumedTotalCount, this.Count);
                            break;
                        }

                        if (noMoreMessage)
                        {
                            Logger.InfoFormat("No more message , hit end ,will Sleep(2000), {0}", DateTime.Now);
                            if (cgOptions.SleepTypeWhileAlwaysRead == 0)
                                Thread.Sleep(0);
                            else if (cgOptions.SleepTypeWhileAlwaysRead == 1)
                                Thread.Sleep(2000);        //Best choice is Thread.Sleep(1).  Other 3 choice still make the CPU 100%
                            else if (cgOptions.SleepTypeWhileAlwaysRead == 2)
                                Thread.Yield();
                            else
                            {

                            }
                        }
#endif
#if NET4
                        throw new NotSupportedException("Please use .net45 to compile .");
#endif
                    }
                }

                Logger.InfoFormat("Read {0}  will commit offset. {1}", latestTotalCount, DateTime.Now);
                connector.CommitOffsets();

                latestTotalCount = Interlocked.Read(ref ConsumerGroupHelper.totalCount);

                Logger.InfoFormat("Totally read {0}  want {1} . ", latestTotalCount, cgOptions.Count);
                if (this.ThreadID == 0)
                {
                    ConsumerGroupHelper.newOffset = connector.GetOffset(cgOptions.Topic);
                }
            }

            this.resetEvent.Set();
        }
    }
}
