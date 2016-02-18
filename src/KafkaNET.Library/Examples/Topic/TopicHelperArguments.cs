// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.


namespace KafkaNET.Library.Examples
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class TopicHelperArguments : KafkaNETExampleSubArguments
    {
        //[('t', "Topic", DefaultValue = "", Required = false, HelpText = "Topics seperated by comma, empty means dump all.")]
        internal string Topic = string.Empty;
        //[('v', "Timestamp", DefaultValue = "", Required = false, HelpText = "Timestamp.")]
        internal DateTime TimestampInUTC = DateTime.MinValue;
        //[('p', "PartitionIndex", DefaultValue = -1, Required = false, HelpText = "Partition of a topic. ")]
        internal int PartitionIndex = -1;
        //[('f', "File", Required = false, HelpText = "File to save result.")]
        internal string File = string.Empty;

        internal override void Parse(string[] args)
        {
            base.BuildDictionary(args);
            GetString("-z", "--zookeeper", ref this.Zookeeper);
            CheckZookeeperPort();
            if (string.IsNullOrEmpty(this.Zookeeper))
                throw new ArgumentException("Must specify zookeeper with port by -z.  Example: -z localhost:2181");

            GetString("-t", "--topic", ref this.Topic);
            GetString("-f", "--file", ref this.File);
            DateTime timestampVal;
            if (this.ArgumentsOptions.ContainsKey("-v"))
            {
                if (DateTime.TryParse(this.ArgumentsOptions["-v"], out timestampVal))
                {
                    this.TimestampInUTC = DateTime.SpecifyKind(timestampVal, DateTimeKind.Utc);
                }
            }
            else if (this.ArgumentsOptions.ContainsKey("--timestamp"))
            {
                if (DateTime.TryParse(this.ArgumentsOptions["--timestamp"], out timestampVal))
                {
                    this.TimestampInUTC = DateTime.SpecifyKind(timestampVal, DateTimeKind.Utc);
                }
            }
        }

        internal override string GetUsage(bool simple)
        {
            if (simple)
                return "Dump topics metadata, such as: earliest/latest offset, replica, ISR.";
            else
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress [-t Topic] [-v TimeInUTC]  [-p PartitionIndex] [-f FileNameToSaveResult] \r\n", KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.Topic.ToString().ToLowerInvariant());
                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                Optional, topic name, will take all topics if this is empty.\r\n");
                sb.AppendFormat("\t-v TimeInUTC            Optional, timestamp to check offset, in UTC.\r\n");
                sb.AppendFormat("\t-p PartitionIndex       Optional, partition need check, default -1 for all partitions.\r\n");
                sb.AppendFormat("\t-f FileNameToSaveResult Optional, file to save result,  default will not save to file.\r\n");
                sb.AppendFormat("Examples:\r\n");
                sb.AppendFormat("\t{0}  {1} -z machine01:2181  \r\n", KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.Topic.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Dump all topics information for zookeeper  machine01:2181. \r\n\r\n");
                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  -v \"2015-01-23 18:00:00\"  -p 4  -f .\\TopicMvLogsPartition4AfterUTCTime.txt \r\n", KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.Topic.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dump partition 4  offset information after UTC time 2015-01-23 18:00:00, and also save result to file .\\TopicMvLogsPartition4AfterUTCTime.txt. \r\n\r\n");
                return sb.ToString();
            }
        }
    }
}
