// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Helper;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class ProducePerfTestHelper : PerfTestHelperBase
    {
        private static List<ProducerData<byte[], Message>> listOfDataNeedSendInOneBatch = new List<ProducerData<byte[], Message>>();

        internal void Run(ProducePerfTestHelperOption option)
        {
            produceOption = option;
            ProducePerfTestKafkaSimpleManagerWrapper.produceOptions = option;

            Initializer(option);
            Thread[] threads = new Thread[option.ThreadCount];
            AutoResetEvent[] autos = new AutoResetEvent[option.ThreadCount];
            threadPareamaters = new PerfTestThreadParameter[option.ThreadCount];

            Logger.InfoFormat("start send {0} ", DateTime.Now);
            stopWatch.Restart();
            //[('p',   -1  random partition.      -2  by default or customized partitioner class. (you need specify PartitionerClass by -l)   >=0 to specific partition.
            for (int i = 0; i < option.ThreadCount; i++)
            {
                AutoResetEvent Auto = new AutoResetEvent(false);
                PerfTestThreadParameter p = new PerfTestThreadParameter();
                p.ThreadId = i;
                p.SpeedConstrolMBPerSecond = (option.SpeedConstrolMBPerSecond * 1.0) / option.ThreadCount;
                p.EventOfFinish = Auto;
                if (produceOption.PartitionId < 0)
                {
                    p.RandomReturnIfNotExist = true;
                }
                else
                    p.RandomReturnIfNotExist = false;
                p.PartitionId = produceOption.PartitionId;
                p.Topic = produceOption.Topic;

                Logger.InfoFormat("Thread:{0} PartitionID:{1} RandomReturnIfNotExist:{2} ", p.ThreadId, p.PartitionId, p.RandomReturnIfNotExist);
                Thread t = new Thread(new ParameterizedThreadStart(RunOneThread));
                t.Start(p);
                autos[i] = Auto;
                threadPareamaters[i] = p;
            }

            WaitHandle.WaitAll(autos);
            stopWatch.Stop();
            Statistics();
        }

        private void RunOneThread(object parameter)
        {
            PerfTestThreadParameter p = (PerfTestThreadParameter)parameter;
            Producer<byte[], Message> producer = ProducePerfTestKafkaSimpleManagerWrapper.Instance.GetProducerMustReturn(p.Topic, p.PartitionId, p.ThreadId, produceOption.SleepInMSWhenException, p.RandomReturnIfNotExist);
            Logger.InfoFormat("==========Thread:{0} PartitionID:{1} RandomReturnIfNotExist:{2}  AssignedProducer:{3}==============", p.ThreadId, p.PartitionId, p.RandomReturnIfNotExist, producer.Config.ForceToPartition);

            DateTime startTime = DateTime.UtcNow;
            while (true)
            {
                p.RequestIndex++;
                try
                {
                    p.StopWatchForSuccessRequest.Restart();
                    producer.Send(listOfDataNeedSendInOneBatch);
                    p.StopWatchForSuccessRequest.Stop();
                    p.SuccRecordsTimeInms += p.StopWatchForSuccessRequest.ElapsedMilliseconds;

                    p.RequestSucc++;
                    p.SentSuccBytes += produceOption.MessageCountPerBatch * produceOption.MessageSize;
                }
                catch (Exception e)
                {
                    p.RequestFail++;
                    Logger.ErrorFormat("Exception:{0}", e.ToString());
                    producer = ProducePerfTestKafkaSimpleManagerWrapper.Instance.GetProducerMustReturn(p.Topic, p.PartitionId, p.ThreadId, produceOption.SleepInMSWhenException, p.RandomReturnIfNotExist);
                    Logger.InfoFormat("==========Thread:{0} PartitionID:{1} RandomReturnIfNotExist:{2}  AssignedProducer:{3}==============", p.ThreadId, p.PartitionId, p.RandomReturnIfNotExist, producer.Config.ForceToPartition);
                }

                if (p.ThreadId == 0 && p.RequestIndex % 100 == 0)
                    Statistics();
                if (produceOption.BatchCount > 0 && p.RequestIndex >= produceOption.BatchCount)
                    break;

                if (p.SpeedConstrolMBPerSecond > 0.0)
                {
                    while (true)
                    {
                        double expectedSuccBytes = (DateTime.UtcNow - startTime).TotalSeconds * p.SpeedConstrolMBPerSecond * 1024 * 1024;
                        if (p.SentSuccBytes > expectedSuccBytes)
                        {
                            Thread.Sleep(20);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }

            p.EventOfFinish.Set();
        }

        private static void Initializer(ProducePerfTestHelperOption producewrapperOption)
        {
            Logger.InfoFormat("prepare perf test, {0} ", DateTime.Now);
            listOfDataNeedSendInOneBatch = new List<ProducerData<byte[], Message>>();
            for (int i = 0; i < producewrapperOption.MessageCountPerBatch; i++)
            {
                String vKey = KafkaClientHelperUtils.GetRandomString(32);
                byte[] bKey = System.Text.Encoding.UTF8.GetBytes(vKey);

                String val = KafkaClientHelperUtils.GetRandomString(producewrapperOption.MessageSize);
                byte[] bVal = System.Text.Encoding.UTF8.GetBytes(val);

                Message message = new Message(bVal, bKey, producewrapperOption.CompressionCodec);
                listOfDataNeedSendInOneBatch.Add(new ProducerData<byte[], Message>(producewrapperOption.Topic, message));
            }
        }
    }
}
