// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Messages;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;

    internal class KafkaNETExampleCommandVerb
    {
        internal static string Verb { get; private set; }
        internal static KafkaNETExampleSubArguments ActiveSubOption { get; private set; }
        internal static string AssemblyName = System.Reflection.Assembly.GetExecutingAssembly().GetName().Name + ".exe";

        internal KafkaNETExampleCommandVerb() { }

        internal static string GetUsage()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("{0} is one utility tool to try KafkaNET.Library.\r\n", AssemblyName);
            sb.AppendFormat("Usage:\r\n", AssemblyName);
            sb.AppendFormat("{0}  <Verb>  ArgumentsAndOpitons \r\n", AssemblyName);
            sb.AppendFormat("Valid verbs includes: \r\n\r\n");
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", KafkaNETExampleType.Topic.ToString().ToLowerInvariant(), new TopicHelperArguments().GetUsage(true));
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", KafkaNETExampleType.ConsumeSimple.ToString().ToLowerInvariant(), new ConsumeDataHelperArguments().GetUsage(true));
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", KafkaNETExampleType.ConsumeGroup.ToString().ToLowerInvariant(), new ConsumeGroupMonitorHelperOptions().GetUsage(true));
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", KafkaNETExampleType.ConsumeGroupMonitor.ToString().ToLowerInvariant(), new ConsumeGroupMonitorHelperOptions().GetUsage(true));
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", KafkaNETExampleType.ProduceSimple.ToString().ToLowerInvariant(), new ProduceSimpleHelperOption().GetUsage(true));
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", KafkaNETExampleType.ProducePerfTest.ToString().ToLowerInvariant(), new ProducePerfTestHelperOption().GetUsage(true));
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", KafkaNETExampleType.EventServerPerfTest.ToString().ToLowerInvariant(), new JavaEventServerPerfTestHelperOptions().GetUsage(true));
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", KafkaNETExampleType.ProduceMonitor.ToString().ToLowerInvariant(), new ProduceMonitorHelperOptions().GetUsage(true));
            sb.AppendFormat("\t{0,-30}  {1}\r\n\r\n", "test", new TestHelperOptions().GetUsage(true));
            return sb.ToString();
        }

        internal void Parse(string[] args)
        {
            if (args == null || args.Length <= 0)
            {
                throw new ArgumentException("Please provide verb.");
            }
            Verb = args[0].ToLowerInvariant();
            if ("topic" == Verb.ToLowerInvariant())
                ActiveSubOption = new TopicHelperArguments();
            else if ("dumpdata" == Verb.ToLowerInvariant()
                 || KafkaNETExampleType.ConsumeSimple.ToString().ToLowerInvariant() == Verb.ToLowerInvariant())
                ActiveSubOption = new ConsumeDataHelperArguments();
            else if ("dumpdataasconsumergroup" == Verb.ToLowerInvariant()
                || KafkaNETExampleType.ConsumeGroup.ToString().ToLowerInvariant() == Verb.ToLowerInvariant())
                ActiveSubOption = new ConsumeGroupHelperOptions();
            else if ("latestoffsetofconsumergroup" == Verb.ToLowerInvariant()
                || "consumegroupm" == Verb.ToLowerInvariant()
                || "consumem" == Verb.ToLowerInvariant()
                || KafkaNETExampleType.ConsumeGroupMonitor.ToString().ToLowerInvariant() == Verb.ToLowerInvariant())
                ActiveSubOption = new ConsumeGroupMonitorHelperOptions();
            else if ("produceroundrobin" == Verb.ToLowerInvariant()
                || KafkaNETExampleType.ProduceSimple.ToString().ToLowerInvariant() == Verb.ToLowerInvariant())
                ActiveSubOption = new ProduceSimpleHelperOption();
            else if ("test" == Verb.ToLowerInvariant())
                ActiveSubOption = new TestHelperOptions();
            else if ("producewrapper" == Verb.ToLowerInvariant()
                || KafkaNETExampleType.ProducePerfTest.ToString().ToLowerInvariant() == Verb.ToLowerInvariant())
                ActiveSubOption = new ProducePerfTestHelperOption();
            else if ("producem" == Verb.ToLowerInvariant()
                || KafkaNETExampleType.ProduceMonitor.ToString().ToLowerInvariant() == Verb.ToLowerInvariant())
                ActiveSubOption = new ProduceMonitorHelperOptions();
            else if (KafkaNETExampleType.EventServerPerfTest.ToString().ToLowerInvariant() == Verb.ToLowerInvariant())
                ActiveSubOption = new JavaEventServerPerfTestHelperOptions();
            else
            {
                throw new ArgumentException(string.Format("The command verb {0} is not recoganized.", Verb));
            }
        }
    }
}
