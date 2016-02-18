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

    /// <summary>
    /// ConsumeGroup
    /// TODO: Currently this example only support consume from beginning of partitions. Can be extend.
    /// </summary>
    internal class ConsumeGroupHelperOptions : ConsumeDataHelperArguments
    {
        ////[('g', "ConsumerGroupName", Required 
        internal string ConsumerGroupName;
        ////[('d', "ConsumerIdPrefix", Required 
        internal string ConsumerId;
        ////[('p', "FetchThreadCountPerConsumer", DefaultValue = 1, Required = false, HelpText = "FetchThreadCountPerConsumer.")]
        internal int FetchThreadCountPerConsumer = 1;
        ////[('j', "Timeout", Required = false,
        internal int Timeout = ConsumerConfiguration.DefaultTimeout;
        ////[('y', "SleepTypeWhileAlwaysRead", DefaultValue = 1, Required = false, HelpText = "SleepTypeWhileAlwaysRead  0: Sleep(0)  1: Sleep(1)  2: Thread.Yield()  Other value: Do nothing")]
        internal int SleepTypeWhileAlwaysRead = 1;
        ////[('x', "CommitEveryBatch",
        internal int CommitBatchSize = KafkaNETExampleConstants.DefaultCommitBatchSize;
        //q
        internal int MaxFetchBufferLength = KafkaNETExampleConstants.DefaultMaxFetchBufferLength;
        //n
        internal int CancellationTimeoutMs = KafkaNETExampleConstants.DefaultCancellationTimeoutMs;
        ////[('v', "CommitOffsetWithPartitionIDOffset",
        internal bool CommitOffsetWithPartitionIDOffset;
        //p
        internal int ZookeeperConnectorCount = 1;
        ////[('s', 
        internal bool UseSharedStaticZookeeperClient = true;
        //k
        internal int[] ZookeeperConnectorConsumeMessageCount;

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

            GetInt("-i", "--BufferSize", ref this.BufferSize);
            GetInt("-h", "--FetchSize", ref this.FetchSize);
            GetInt("-w", "--MaxWaitTime", ref this.MaxWaitTime);
            GetInt("-e", "--MinWaitBytes", ref this.MinWaitBytes);
            GetString("-f", "--File", ref this.File);

            GetInt("-c", "--Count", ref this.Count);
            GetString("-g", "--ConsumerGroupName", ref this.ConsumerGroupName);
            if (string.IsNullOrEmpty(this.ConsumerGroupName))
                throw new ArgumentException("Must specify topic by -g.");

            GetString("-d", "--ConsumerId", ref this.ConsumerId);
            if (string.IsNullOrEmpty(this.ConsumerId))
                throw new ArgumentException("Must specify topic by -d.");
            GetInt("-r", "--FetchThreadCountPerConsumer", ref this.FetchThreadCountPerConsumer);

            GetInt("-j", "--Timeout", ref this.Timeout);
            GetInt("-y", "--SleepTypeWhileAlwaysRead", ref this.SleepTypeWhileAlwaysRead);
            GetInt("-x", "--CommitBatchSize", ref this.CommitBatchSize);
            GetInt("-q", "--MaxFetchBufferLength", ref this.MaxFetchBufferLength);
            GetBool("-v", "--CommitOffsetWithPartitionIDOffset", ref this.CommitOffsetWithPartitionIDOffset);

            GetInt("-n", "--CancellationTimeoutMs", ref this.CancellationTimeoutMs);

            GetInt("-p", "--ZookeeperConnectorCount", ref this.ZookeeperConnectorCount);
            GetBool("-s", "--UseSharedStaticZookeeperClient", ref this.UseSharedStaticZookeeperClient);

            string tempZookeeperConnectorConsumeMessageCount = string.Empty;
            GetString("-k", "--ZookeeperConnectorConsumeMessageCount", ref tempZookeeperConnectorConsumeMessageCount);
            ZookeeperConnectorConsumeMessageCount = new int[ZookeeperConnectorCount];
            string[] tempArray = tempZookeeperConnectorConsumeMessageCount.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < ZookeeperConnectorConsumeMessageCount.Length; i++)
                ZookeeperConnectorConsumeMessageCount[i] = -1;
            for (int i = 0; i < tempArray.Length; i++)
                ZookeeperConnectorConsumeMessageCount[i] = Convert.ToInt32(tempArray[i]);
        }

        internal override string GetUsage(bool simple)
        {

            if (simple)
                return "Consume data in single thread.";
            else
            {
                StringBuilder sb = new StringBuilder();

                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress -t Topic -g GroupName -d ConsumerID \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ConsumeGroup.ToString().ToLowerInvariant());

                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                Required, topic name.\r\n");
                sb.AppendFormat("\t-g ConsumerGroupName    Required, ConsumerGroupName.\r\n");
                sb.AppendFormat("\t-d ConsumerId           Required, ConsumerId.\r\n");

                sb.AppendFormat("\t-i BufferSize           Optional, BufferSize, default :{0}.\r\n", KafkaSimpleManagerConfiguration.DefaultBufferSize);
                sb.AppendFormat("\t-w MaxWaitTime          Optional, MaxWaitTime, default 0.\r\n");
                sb.AppendFormat("\t-e MinWaitBytes         Optional, MinWaitBytes, default 0.\r\n");
                sb.AppendFormat("\t-c Count                Optional, Count, default -1 for all.\r\n");

                sb.AppendFormat("\t-r FetchThreadCountPerConsumer  Optional, FetchThreadCountPerConsumer, default 1.\r\n");
                sb.AppendFormat("\t-j                         Optional, Timeout.  -1 for block, 0 for asap, default value :{0}\r\n", ConsumerConfiguration.DefaultTimeout);
                sb.AppendFormat("\t-y                         Optional, SleepTypeWhileAlwaysRead   0: Sleep(0)  1: Sleep(1)  2: Thread.Yield()  Other value: Do nothing   Default value 1.\r\n");
                sb.AppendFormat("\t-x                         Optional, CommitBatchSize, default {0}\r\n", KafkaNETExampleConstants.DefaultCommitBatchSize);
                sb.AppendFormat("\t-q                         Optional, MaxFetchBufferLength. default {0}\r\n", KafkaNETExampleConstants.DefaultMaxFetchBufferLength);
                sb.AppendFormat("\t-v                         Optional, CommitOffsetWithPartitionIDOffset, default false\r\n");
                sb.AppendFormat("\t-n                         Optional, CancellationTimeoutMs, default {0} means not use cancelToken.\r\n", KafkaNETExampleConstants.DefaultCancellationTimeoutMs);
                sb.AppendFormat("\t-p                         Optional, ZookeeperConnectorCount, default 1 \r\n");
                sb.AppendFormat("\t-k                         Optional, ZookeeperConnectorConsumeMessageCount,  comma seperated count for eache zookeeper connector, default -1 \r\n");
                sb.AppendFormat("\t-s                         Optional, UseSharedStaticZookeeperClient, default true\r\n");

                sb.AppendFormat("Examples:\r\n");

                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs -g G1 -d C1  \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ConsumeGroup.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop consume group all partitions. \r\n\r\n");

                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs -g G1 -d C1 -x 100 -n 3000 \r\n"
                 , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ConsumeGroup.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop consume group all partitions.  With Cancellation token, if can't get 100 messages in 3000 ms, will cancel.  And commit offset every 100 messages.   \r\n");

                return sb.ToString();
            }
        }


    }
}
