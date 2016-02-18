// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class TestHelperOptions : KafkaNETExampleSubArguments
    {
        ////[('t', "Topic", Required = true, HelpText = "Topic.")]
        public string Topic;

        ////[('c', "Case", Required = true, HelpText = "Case")]
        public string Case;

        ////[('m', "MessageSize", DefaultValue = 4096, Required = false, HelpText = "Message size.")]
        public int MessageSize = KafkaNETExampleConstants.DefaultMessageSize;

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

            GetString("-c", "--case", ref this.Case);
            if (string.IsNullOrEmpty(this.Case))
                throw new ArgumentException("Must specify case by -c.");
            GetInt("-m", "--MessageSize", ref this.MessageSize);
        }

        internal override string GetUsage(bool simple)
        {
            if (simple)
                return "Run some adhoc test cases.";
            else
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress -t Topic -c CaseName [-m MessageSize] \r\n", KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.Test.ToString().ToLowerInvariant());
                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                Required, topic name.\r\n");
                sb.AppendFormat("\t-c CaseName             Required, Valid values:  Bug1490652 \r\n");
                sb.AppendFormat("\t-m MessageSize          Optional, Message size, default value {0}.\r\n", KafkaNETExampleConstants.DefaultMessageSize);
                sb.AppendFormat("Examples:\r\n");
                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs -c  Bug1490652 \r\n", KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.Test.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Run test test case Bug1490652 for zookeeper  machine01:2181. \r\n\r\n");
                return sb.ToString();
            }
        }
    }
}
