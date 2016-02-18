// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class ProduceMonitorHelperOptions : KafkaNETExampleSubArguments
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumeDataHelperArguments));
        //[('t', "Topic", Required = true, HelpText = "Topic.")]
        internal string Topic;
        //[('f', "File", Required = false, HelpText = "File.")]
        internal string File = "LatestOffsetMonitor.txt";

        internal int IntervalInSeconds = KafkaNETExampleConstants.DefaultIntefvalInSeconds;

        internal override void Parse(string[] args)
        {
            base.BuildDictionary(args);
            GetString("-z", "--zookeeper", ref this.Zookeeper);
            CheckZookeeperPort();
            if (string.IsNullOrEmpty(this.Zookeeper))
                throw new ArgumentException("Must specify zookeeper with port by -z.  Example: -z localhost:2181");

            GetString("-t", "--topic", ref this.Topic);
            if (string.IsNullOrEmpty(this.Topic))
                throw new ArgumentException("Must specify topic name by -t");
            GetString("-f", "--file", ref this.File);

            GetInt("-i", "--interval", ref this.IntervalInSeconds);
        }

        internal override string GetUsage(bool simple)
        {
            if (simple)
                return "Monitor latest offset.";
            else
            {
                StringBuilder sb = new StringBuilder();

                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress -t Topic  [-i IntervalInSeconds] [-f File] \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProduceMonitor.ToString().ToLowerInvariant());

                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                Required, topic name\r\n");
                sb.AppendFormat("\t-i IntervalInSeconds    Optional, IntervalInSeconds defalut value  {0}\r\n", KafkaNETExampleConstants.DefaultIntefvalInSeconds);
                sb.AppendFormat("\t-f File                 Optional, file, defalut value  {0}\r\n", this.File);
                sb.AppendFormat("Examples:\r\n");

                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.ProduceMonitor.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop dump latest offset and speed. Interval:{0}\r\n\r\n", KafkaNETExampleConstants.DefaultIntefvalInSeconds);
                return sb.ToString();
            }

        }
    }
}
