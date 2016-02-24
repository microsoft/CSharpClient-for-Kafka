// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Messages;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class ProduceSimpleHelperOption : KafkaNETExampleSubArguments
    {
        //[('t', "Topic", Required = true, HelpText = "Topic.")]
        internal string Topic;
        //[('p',   -1  random partition.      -2  by default or customized partitioner class. (you need specify PartitionerClass by -l)   >=0 to specific partition.
        internal int PartitionId = KafkaNETExampleConstants.DefaultProducePartitionId;
        //[('l', "PartitionerClass"
        internal string PartitionerClass;
        //[('c', "BatchCount", DefaultValue = 100000, Required = false, HelpText = "Number of batch to produce. -1:infinite. ")]
        internal int BatchCount = -1;
        //[('b', "MessageCountPerBatch", DefaultValue = 1, Required = false, HelpText = "Batch size.")]
        internal int MessageCountPerBatch = KafkaNETExampleConstants.DefaultMessageCountPerBatch;
        //[('m', "MessageSize", DefaultValue = 4096, Required = false, HelpText = "Message size.")]
        internal int MessageSize = KafkaNETExampleConstants.DefaultMessageSize;
        //[('r', "Compression", DefaultValue = "0", Required = false, HelpText = "Compression:     0 or empty: No ,     1: Gzip, 2 Snappy   ")]
        internal int Compression = 0;
        internal CompressionCodecs CompressionCodec = CompressionCodecs.NoCompressionCodec;
        //[('a'
        internal short RequiredAcks = KafkaNETExampleConstants.DefaultRequiredAcks;//SyncProducerConfiguration.DefaultRequiredAcks;
        //[('k'
        internal int AckTimeout = SyncProducerConfiguration.DefaultAckTimeout;
        //[('s'
        internal int SendTimeout = SyncProducerConfiguration.DefaultSendTimeout;
        //[('e'
        internal int ReceiveTimeout = SyncProducerConfiguration.DefaultReceiveTimeout;
        //[('i'
        internal int BufferSize = SyncProducerConfiguration.DefaultBufferSize;
        //[('y', "SyncProducerOfOneBroker", DefaultValue = 1, Required = false, HelpText = "SyncProducerOfOneBroker value.")]
        internal int SyncProducerOfOneBroker = ProducerConfiguration.DefaultSyncProducerOfOneBroker;
        //[('w', "ConstantMessageKey
        internal bool ConstantMessageKey = false;

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
            GetBool("-w", "--ConstantMessageKey", ref this.ConstantMessageKey);
            if (this.PartitionId == -2)
            {
                if (string.IsNullOrEmpty(this.PartitionerClass))
                    throw new ArgumentException(string.Format("You specify partitionID as -2, please also specify the partitioner class full name, for example: {0}", ProducerConfiguration.DefaultPartitioner));
            }
        }

        internal override string GetUsage(bool simple)
        {
            if (simple)
                return "Produce data in single thread.";
            else
            {
                StringBuilder sb = new StringBuilder();

                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress [-t Topic] [-c BatchCount]  [-b MessageCountPerBatch] [-m MessageSize] [] \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProduceSimple.ToString().ToLowerInvariant());
                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                Optional, topic name, will take all topics if this is empty.\r\n");
                sb.AppendFormat("\t-p PartitionId          Optional, Partition to write,  default -1. -1  random partition. \r\n");
                sb.AppendFormat("\t                                                                   -2  by default or customized partitioner class. (you need specify PartitionerClass by -l) \r\n");
                sb.AppendFormat("\t                                                                   >=0 to specific partition. \r\n");
                sb.AppendFormat("\t-l PartitionerClass     Optional, PartitionerClass name. Default value is empty.  The partitioner class for by hash of key is: {0}\r\n", ProducerConfiguration.DefaultPartitioner);
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
                sb.AppendFormat("Examples:\r\n");
                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProduceSimple.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop send batch to random partition, each batch contains {0} messages, each message has {1} bytes. NoCompression. \r\n\r\n"
                    , KafkaNETExampleConstants.DefaultMessageCountPerBatch, KafkaNETExampleConstants.DefaultMessageSize);
                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  -p -1  -c 20 \r\n"
                   , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProduceSimple.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, send 20 batch to one random partition, each batch contains {0} messages, each message has {1} bytes. NoCompression. \r\n\r\n"
                    , KafkaNETExampleConstants.DefaultMessageCountPerBatch, KafkaNETExampleConstants.DefaultMessageSize);
                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  -p 2  -c 20 \r\n"
                  , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProduceSimple.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, send 20 batch to one partition 2, each batch contains {0} messages, each message has {1} bytes. NoCompression. \r\n\r\n"
                    , KafkaNETExampleConstants.DefaultMessageCountPerBatch, KafkaNETExampleConstants.DefaultMessageSize);
                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  -p -2 -l Kafka.Client.Producers.Partitioning.DefaultPartitioner`1 -c 20  \r\n"
                  , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProduceSimple.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181,   send 20 batch partition by default partitioner abs(key hash mod partition number), each batch contains {0} messages, each message has {1} bytes. NoCompression. \r\n\r\n"
                    , KafkaNETExampleConstants.DefaultMessageCountPerBatch, KafkaNETExampleConstants.DefaultMessageSize);
                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  -p -2 -l KafkaNET.Library.Examples.CustomePartitionerSample`1 -c 20 \r\n"
                  , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProduceSimple.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181,  send 20 batch partition by custom partitioner abs(2*key hash mod partition number/3), each batch contains {0} messages, each message has {1} bytes. NoCompression. \r\n\r\n"
                    , KafkaNETExampleConstants.DefaultMessageCountPerBatch, KafkaNETExampleConstants.DefaultMessageSize);

                return sb.ToString();
            }
        }
    }
}
