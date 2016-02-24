// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Cfg;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class ProducePerfTestHelperOption : ProduceSimpleHelperOption
    {
        ////[('d', "ThreadCount", DefaultValue = 16, Required = false, HelpText = "Number of task to run test")]
        internal int ThreadCount = KafkaNETExampleConstants.DefaultProduceThreadCount;
        //-o
        internal int SpeedConstrolMBPerSecond = 0;
        //-f
        internal int SleepInMSWhenException = KafkaNETExampleConstants.DefaultProduceSleepMsOnceException;

        internal override void Parse(string[] args)
        {
            base.BuildDictionary(args);
            GetString("-z", "--zookeeper", ref this.Zookeeper);
            CheckZookeeperPort();
            if (string.IsNullOrEmpty(this.Zookeeper))
                throw new ArgumentException("Must specify zookeeper with port by -z.  Example: -z localhost:2181");
            GetString("-t", "--topic", ref this.Topic);
            if (string.IsNullOrEmpty(this.Topic))
                throw new ArgumentException("Must specify topic by -t.");

            GetInt("-p", "--PartitionId", ref this.PartitionId);
            GetString("-l", "--PartitionerClass", ref this.PartitionerClass);
            GetInt("-c", "--batchcount", ref this.BatchCount);
            GetInt("-b", "--MessageCountPerBatch", ref this.MessageCountPerBatch);
            GetInt("-m", "--MessageSize", ref this.MessageSize);
            GetInt("-r", "--Compression", ref this.Compression);
            this.CompressionCodec = KafkaNetLibraryExample.ConvertToCodec(this.Compression.ToString());
            GetShort("-a", "--RequiredAcks", ref this.RequiredAcks);
            GetInt("-k", "--AckTimeout", ref this.AckTimeout);
            GetInt("-s", "--SendTimeout", ref this.SendTimeout);
            GetInt("-e", "--ReceiveTimeout", ref this.ReceiveTimeout);
            GetInt("-i", "--BufferSize", ref this.BufferSize);
            GetInt("-y", "--SyncProducerOfOneBroker", ref this.SyncProducerOfOneBroker);
            GetInt("-d", "--ThreadCount", ref this.ThreadCount);
            GetInt("-o", "--SpeedConstrolMBPerSecond", ref this.SpeedConstrolMBPerSecond);
            GetInt("-f", "--SleepInMSWhenException", ref this.SleepInMSWhenException);
            if (this.PartitionId == -2)
            {
                if (string.IsNullOrEmpty(this.PartitionerClass))
                    throw new ArgumentException(string.Format("You specify partitionID as -2, please also specify the partitioner class full name, for example: {0}", ProducerConfiguration.DefaultPartitioner));
            }
        }

        internal override string GetUsage(bool simple)
        {
            if (simple)
            {
                return "Produce data in multiple thread.";
            }
            else
            {
                StringBuilder sb = new StringBuilder();

                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress -t Topic \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProducePerfTest.ToString().ToLowerInvariant());
                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                Required, topic name\r\n");
                sb.AppendFormat("\t-c BatchCount           Optional, BatchCount to write,  default -1 means always write.\r\n");
                sb.AppendFormat("\t-b MessageCountPerBatch Optional, MessageCountPerBatch, default {0}.\r\n", KafkaNETExampleConstants.DefaultMessageCountPerBatch);
                sb.AppendFormat("\t-m MessageSize          Optional, message size, default :{0}\r\n", KafkaNETExampleConstants.DefaultMessageSize);
                sb.AppendFormat("\t-r Compression          Optional, Compression mode, default 0, no Compression.   1 for GZIP, 2 for SNAPPY\r\n");
                sb.AppendFormat("\t-a RequiredAcks         Optional, RequiredAcks, default :{0}.\r\n", KafkaNETExampleConstants.DefaultRequiredAcks);
                sb.AppendFormat("\t-k AckTimeout           Optional, AckTimeout, default :{0}.\r\n", SyncProducerConfiguration.DefaultAckTimeout);
                sb.AppendFormat("\t-s SendTimeout          Optional, SendTimeout, default :{0}.\r\n", SyncProducerConfiguration.DefaultSendTimeout);
                sb.AppendFormat("\t-e ReceiveTimeout       Optional, ReceiveTimeout, default :{0}.\r\n", SyncProducerConfiguration.DefaultReceiveTimeout);
                sb.AppendFormat("\t-i BufferSize           Optional, BufferSize, default :{0}.\r\n", SyncProducerConfiguration.DefaultBufferSize);
                sb.AppendFormat("\t-y SyncProducerOfOneBroker  Optional, SyncProducerOfOneBroker, default :{0}.\r\n", ProducerConfiguration.DefaultSyncProducerOfOneBroker);
                sb.AppendFormat("\t-d ThreadCount              Optional, ThreadCount, default :{0}.\r\n", KafkaNETExampleConstants.DefaultProduceThreadCount);
                sb.AppendFormat("\t-o SpeedConstrolMBPerSecond Optional, SpeedConstrolMBPerSecond,  default :{0}.\r\n", 0);
                sb.AppendFormat("\t-f SleepInMSWhenException Optional,   SleepInMSWhenException,  default :{0}.\r\n", KafkaNETExampleConstants.DefaultProduceSleepMsOnceException);
                sb.AppendFormat("\t-p PartitionId          Optional, Partition to write,  default -1. -1  random partition. \r\n");
                sb.AppendFormat("\t                                                                   -2  by default or customized partitioner class. (you need specify PartitionerClass by -l) \r\n");
                sb.AppendFormat("\t                                                                   >=0 to specific partition. \r\n");
                sb.AppendFormat("\t-l PartitionerClass     Optional, PartitionerClass name. Default value is empty.  The partitioner class for by hash of key is: {0}\r\n", ProducerConfiguration.DefaultPartitioner);
                sb.AppendFormat("Examples:\r\n");
                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProducePerfTest.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop send batch in {2} threads, each batch contains {0} messages, each message has {1} bytes. NoCompression. \r\n\r\n"
                    , KafkaNETExampleConstants.DefaultMessageCountPerBatch, KafkaNETExampleConstants.DefaultMessageSize, KafkaNETExampleConstants.DefaultProduceThreadCount);

                return sb.ToString();
            }
        }
    }
}
