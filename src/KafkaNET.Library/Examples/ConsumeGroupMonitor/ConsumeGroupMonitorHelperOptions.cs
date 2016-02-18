// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class ConsumeGroupMonitorHelperOptions : KafkaNETExampleSubArguments
    {
        //[('t', "topic", Required = true, HelpText = "topic.")]
        internal string Topic;
        //[('g', "ConsumerGroupName", Required 
        internal string ConsumerGroupName = string.Empty;
        //i
        internal int IntervalInSeconds = KafkaNETExampleConstants.DefaultIntefvalInSeconds;
        //[('f', "File", Required = false, HelpText = "File.")]
        internal string File = "ConsumeGroupMonitor.txt";
        //a   Exmaple  G1:t1,G2:t2
        internal string ConsumeGroupTopicArray;
        //r
        internal int RefreshConsumeGroupIntervalInSeconds = KafkaNETExampleConstants.DefaultRefreshConsumeGroupIntervalInSeconds;

        internal override void Parse(string[] args)
        {
            base.BuildDictionary(args);
            GetString("-z", "--zookeeper", ref this.Zookeeper);
            CheckZookeeperPort();
            if (string.IsNullOrEmpty(this.Zookeeper))
                throw new ArgumentException("Must specify zookeeper with port by -z.  Example: -z localhost:2181");

            GetString("-f", "--file", ref this.File);
            GetString("-a", "--ConsumeGroupTopicArray", ref this.ConsumeGroupTopicArray);
            if (string.IsNullOrEmpty(this.ConsumeGroupTopicArray))
            {
                GetString("-t", "--topic", ref this.Topic);
                GetString("-g", "--ConsumerGroupName", ref this.ConsumerGroupName);
            }
            else
            {
                this.File = this.File.Replace(".txt", string.Empty);
            }

            GetInt("-i", "--IntervalInSeconds", ref this.IntervalInSeconds);
            GetInt("-r", "--RefreshConsumeGroupIntervalInSeconds", ref this.RefreshConsumeGroupIntervalInSeconds);
        }

        internal override string GetUsage(bool simple)
        {
            if (simple)
                return "Monitor latest offset gap and speed of consumer group.";
            else
            {
                StringBuilder sb = new StringBuilder();

                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress [-t Topic  -g Group] [-a ConsumeGroupTopicArray] [-i IntervalInSeconds] [-f File] \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ConsumeGroupMonitor.ToString().ToLowerInvariant());

                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                    Optional, topic name\r\n");
                sb.AppendFormat("\t-g Group name               Optional, Group name\r\n");
                sb.AppendFormat("\t-a ConsumeGroupTopicArray   Optional, Consume group/topic name array.   Exmaple  G1:t1,G2:t2\r\n");
                sb.AppendFormat("\t-i IntervalInSeconds        Optional, IntervalInSeconds defalut value  {0}\r\n", KafkaNETExampleConstants.DefaultIntefvalInSeconds);
                sb.AppendFormat("\t-r RefreshConsumeGroupIntervalInSeconds        Optional, RefreshConsumeGroupIntervalInSeconds defalut value  {0}\r\n", KafkaNETExampleConstants.DefaultRefreshConsumeGroupIntervalInSeconds);
                sb.AppendFormat("\t-f File                     Optional, file, defalut value  {0}\r\n", this.File);

                sb.AppendFormat("Examples:\r\n");

                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs -g G1 \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ConsumeGroupMonitor.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop dump latest offset gap and speed. Interval:{0}\r\n\r\n", KafkaNETExampleConstants.DefaultIntefvalInSeconds);

                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -a G1:mvlogs,G2:mvlogs2 \r\n"
                   , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ConsumeGroupMonitor.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop dump latest offset gap and speed.  For consume group G1, topic mvlogs.  consume group G2, topic mvlogs2. Interval:{0}\r\n\r\n", KafkaNETExampleConstants.DefaultIntefvalInSeconds);

                return sb.ToString();
            }
        }
    }
}
