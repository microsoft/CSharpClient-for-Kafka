// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    class JavaEventServerPerfTestHelperOptions : ProducePerfTestHelperOption
    {
        //[('s', "Topic", Required = true, HelpText = "Topic.")]
        internal string EventServerFullAddress;
        //[('y', "SendType",")]
        internal string SendType = "multi";

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

            GetInt("-c", "--batchcount", ref this.BatchCount);
            GetInt("-b", "--MessageCountPerBatch", ref this.MessageCountPerBatch);
            GetInt("-m", "--MessageSize", ref this.MessageSize);
            GetInt("-d", "--ThreadCount", ref this.ThreadCount);
            GetString("-s", "--EventServerFullAddress", ref this.EventServerFullAddress);
            if (string.IsNullOrEmpty(this.EventServerFullAddress))
                this.EventServerFullAddress = string.Format("http://{0}:85", this.Zookeeper.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries)[0]);

            GetString("-y", "--SendType", ref this.SendType);
            if (this.SendType != "single" && this.SendType != "multi")
                throw new ArgumentException("SendType can only be multi or single");
            GetInt("-o", "--SpeedConstrolMBPerSecond", ref this.SpeedConstrolMBPerSecond);
            GetInt("-f", "--SleepInMSWhenException", ref this.SleepInMSWhenException);
        }

        internal override string GetUsage(bool simple)
        {
            if (simple)
                return "Http Post data to event server in multiple thread.";
            else
            {
                StringBuilder sb = new StringBuilder();

                sb.AppendFormat("\r\n{0}  {1} -z ZookeeperAddress -t Topic -s EventServerFullAddress \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.EventServerPerfTest.ToString().ToLowerInvariant());

                sb.AppendFormat("Parameters:\r\n");
                sb.AppendFormat("\t-z ZookeeperAddress     Required, zookeeper address.\r\n");
                sb.AppendFormat("\t-t Topic                Required, topic name\r\n");
                sb.AppendFormat("\t-s EventServerFullAddress  Required, EventServerFullAddress  example: http://localhost:85\r\n");
                sb.AppendFormat("\t-y SendType             Optional, SendType. multi or single            Default multi\r\n");
                sb.AppendFormat("\t-c BatchCount           Optional, BatchCount to write in each thread,  default -1 means always write.\r\n");
                sb.AppendFormat("\t-b MessageCountPerBatch Optional, MessageCountPerBatch, default {0}.\r\n", KafkaNETExampleConstants.DefaultMessageCountPerBatch);
                sb.AppendFormat("\t-m MessageSize          Optional, message size, default :{0}\r\n", KafkaNETExampleConstants.DefaultMessageSize);
                sb.AppendFormat("\t-d ThreadCount          Optional, ThreadCount,  default :{0}.\r\n", KafkaNETExampleConstants.DefaultProduceThreadCount);
                sb.AppendFormat("\t-o SpeedConstrolMBPerSecond Optional, SpeedConstrolMBPerSecond,  default :{0}.\r\n", 0);
                sb.AppendFormat("\t-f SleepInMSWhenException Optional,   SleepInMSWhenException,  default :{0}.\r\n", KafkaNETExampleConstants.DefaultProduceSleepMsOnceException);

                sb.AppendFormat("Examples:\r\n");

                sb.AppendFormat("\t{0}  {1} -z machine01:2181 -t mvlogs  -s http://machine01:85 \r\n"
                    , KafkaNETExampleCommandVerb.AssemblyName, KafkaNETExampleType.EventServerPerfTest.ToString().ToLowerInvariant());
                sb.AppendFormat("\t\t Connect to  machine01:2181, dead loop send batch in {2} threads, each batch contains {0} messages, each message has {1} bytes. \r\n\r\n"
                    , KafkaNETExampleConstants.DefaultMessageCountPerBatch, KafkaNETExampleConstants.DefaultMessageSize, KafkaNETExampleConstants.DefaultProduceThreadCount);

                return sb.ToString();
            }
        }
    }
}
