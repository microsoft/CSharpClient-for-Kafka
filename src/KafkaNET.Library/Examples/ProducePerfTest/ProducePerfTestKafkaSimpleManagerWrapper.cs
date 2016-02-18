// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Helper;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Microsoft.KafkaNET.Library.Util;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Probably you need copy out this class to manger the KafkaSimpleManager.
    /// </summary>
    internal class ProducePerfTestKafkaSimpleManagerWrapper
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ProducePerfTestKafkaSimpleManagerWrapper));
        internal static ProducePerfTestHelperOption produceOptions = null;
        private static volatile ProducePerfTestKafkaSimpleManagerWrapper instance;
        private static object syncRoot = new Object();

        private object lockForDictionaryChange = new Object();
        private KafkaSimpleManagerConfiguration config;
        private KafkaSimpleManager<byte[], Message> kafkaSimpleManage;
        private ProducerConfiguration producerConfigTemplate;
        private int correlationIDGetProducer = 0;
        private string clientId = "KafkaSimpleManagerProducerWrapper";

        private ProducePerfTestKafkaSimpleManagerWrapper()
        {
            config = new KafkaSimpleManagerConfiguration()
            {
                Zookeeper = produceOptions.Zookeeper,
                PartitionerClass = produceOptions.PartitionerClass,
                MaxMessageSize = SyncProducerConfiguration.DefaultMaxMessageSize
            };

            config.Verify();
            producerConfigTemplate = new ProducerConfiguration(
                  new List<BrokerConfiguration>() { }) //The Brokers will be replaced inside of KafkaSimpleManager
            {
                ForceToPartition = -1,
                PartitionerClass = config.PartitionerClass,
                TotalNumPartitions = 0,
                RequiredAcks = produceOptions.RequiredAcks,
                AckTimeout = produceOptions.AckTimeout,
                SendTimeout = produceOptions.SendTimeout,
                ReceiveTimeout = produceOptions.ReceiveTimeout,
                CompressionCodec = KafkaNetLibraryExample.ConvertToCodec(produceOptions.Compression.ToString()),
                BufferSize = produceOptions.BufferSize,
                SyncProducerOfOneBroker = produceOptions.SyncProducerOfOneBroker, //Actually it's sync producer socket count of one partition
                MaxMessageSize = Math.Max(SyncProducerConfiguration.DefaultMaxMessageSize, produceOptions.MessageSize)
            };

            kafkaSimpleManage = new KafkaSimpleManager<byte[], Message>(config);
            int correlationId = Interlocked.Increment(ref correlationIDGetProducer);
            kafkaSimpleManage.InitializeProducerPoolForTopic(0, clientId, correlationId, produceOptions.Topic, true, producerConfigTemplate, true);
        }

        internal static ProducePerfTestKafkaSimpleManagerWrapper Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (syncRoot)
                    {
                        if (instance == null)
                        {
                            instance = new ProducePerfTestKafkaSimpleManagerWrapper();
                        }
                    }
                }

                return instance;
            }
        }

        internal Producer<byte[], Message> GetProducerMustReturn(string topic, int partitionId, int threadId, int sleepms, bool randomReturnIfNotExist)
        {
            Producer<byte[], Message> producer = null;
            while (producer == null)
            {
                producer = kafkaSimpleManage.GetProducerOfPartition(topic, partitionId, randomReturnIfNotExist);
                if (producer == null)
                {
                    Logger.ErrorFormat("No valid producer  Current thread {0} topic:{2} partition:{3} , please check kafka . will sleep: {1}ms  and retry ..", threadId, sleepms, topic, partitionId);
                    Thread.Sleep(sleepms);
                    producer = kafkaSimpleManage.GetProducerOfPartition(topic, partitionId, randomReturnIfNotExist);
                    if (producer == null)
                    {
                        int correlationId = Interlocked.Increment(ref correlationIDGetProducer);
                        kafkaSimpleManage.InitializeProducerPoolForTopic(0, clientId, correlationId, topic, true, producerConfigTemplate, true);
                    }
                    else
                        return producer;
                }
                else
                    return producer;
            }
            return producer;
        }

        #region Dispose
        private volatile bool disposed = false;
        internal void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    lock (lockForDictionaryChange)
                    {
                        Logger.Info("Got lock Will dispose KafkaSimpleManager ...");
                        kafkaSimpleManage.Dispose();
                        kafkaSimpleManage = null;
                        config = null;
                        Logger.Info("Finish dispose KafkaSimpleManager ...will release lock.");
                    }
                }

                disposed = true;
            }
        }
        #endregion
    }
}
