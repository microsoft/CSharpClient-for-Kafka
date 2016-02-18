// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class ConsumeDataHelperArguments : KafkaNETExampleSubArguments
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumeDataHelperArguments));

        //[('t', "Topic", Required = true, HelpText = "Topic.")]
        internal string Topic;
        //[('o', "Offset", DefaultValue = "latest", Required = false, HelpText = "Offset.")]
        internal string Offset = "earliest";
        //[('p', "PartitionIndex", DefaultValue = -1, Required = false, HelpText = "The partition of a topic.")]
        internal int PartitionIndex = -1;
        //[('i'
        internal int BufferSize = KafkaSimpleManagerConfiguration.DefaultBufferSize;
        //h
        internal int FetchSize = KafkaSimpleManagerConfiguration.DefaultFetchSize;
        //w
        internal int MaxWaitTime = 0;
        //e
        internal int MinWaitBytes = 0;
        //[('f', "File", DefaultValue = "ReadlAllMessage.txt", Required = false, HelpText = "File.")]
        internal string File = "ReadlAllMessage.txt";
        //[('m', "LastMessagesCount", DefaultValue = 1, Required = false, HelpText = "Output last message count while OffsetTime is Last Or TimeStamp. ")]
        internal int LastMessagesCount = 10;
        //[('b', "DumpBinaryData", DefaultValue = false, Required = false, HelpText = "DumpBinaryData.")]
        internal bool DumpBinaryData = false;
        //[('u', "DumpDataAsUTF8", DefaultValue = false, Required = false, HelpText = "DumpDataAsUTF8.")]
        internal bool DumpDataAsUTF8 = false;
        //[('c', "Count", DefaultValue = -1, Required = false, HelpText = "Number of messages to dump details. -1:infinite.")]
        internal int Count = -1;

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

            GetString("-o", "--Offset", ref this.Offset);

            GetInt("-p", "--PartitionIndex", ref this.PartitionIndex);
            GetInt("-i", "--BufferSize", ref this.BufferSize);
            GetInt("-h", "--FetchSize", ref this.FetchSize);
            GetInt("-w", "--MaxWaitTime", ref this.MaxWaitTime);
            GetInt("-e", "--MinWaitBytes", ref this.MinWaitBytes);
            GetString("-f", "--File", ref this.File);

            GetInt("-m", "--LastMessagesCount", ref this.LastMessagesCount);
            GetBool("-b", "--DumpBinaryData", ref this.DumpBinaryData);
            GetBool("-u", "--DumpDataAsUTF8", ref this.DumpDataAsUTF8);
            GetInt("-c", "--Count", ref this.Count);
        }

        internal override string GetUsage(bool simple)
        {

            if (simple)
                return "Consume data in single thread.";
            else
            {
                StringBuilder sb = new StringBuilder();

                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress -t Topic \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ConsumeSimple.ToString().ToLowerInvariant());

                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                Required, topic name.\r\n");
                sb.AppendFormat("\t-o Offset               Optional, Offest can be  [<unsigned-integer>|earliest|latest|<yyyy-MM-dd hh:mm:ss>], default value earliest\r\n");
                sb.AppendFormat("\t-p PartitionIndex       Optional, PartitionIndex, default -1 for all partitoins.\r\n");
                sb.AppendFormat("\t-i BufferSize           Optional, BufferSize, default :{0}.\r\n", KafkaSimpleManagerConfiguration.DefaultBufferSize);
                sb.AppendFormat("\t-w MaxWaitTime          Optional, MaxWaitTime, default 0.\r\n");
                sb.AppendFormat("\t-e MinWaitBytes         Optional, MinWaitBytes, default 0.\r\n");
                sb.AppendFormat("\t-f File                 Optional, file, defalut value  {0}\r\n", this.File);

                sb.AppendFormat("\t-m LastMessagesCount    Optional, LastMessagesCount, default 10.\r\n");
                sb.AppendFormat("\t-b DumpBinaryData       Optional, DumpBinaryData, default false.\r\n");
                sb.AppendFormat("\t-u DumpDataAsUTF8       Optional, DumpDataAsUTF8, default false.\r\n");
                sb.AppendFormat("\t-c Count                Optional, Count, default -1 for all.\r\n");

                sb.AppendFormat("Examples:\r\n");

                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ConsumeSimple.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop consume all partitions.  Form last 10 messages.");

                return sb.ToString();
            }
        }
    }
}
