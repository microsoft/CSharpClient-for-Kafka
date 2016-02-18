// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using log4net.Config;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using ZooKeeperNet;

    //TODO: force connect to one machine to dump
    //TODO: force get topic's related brokers from zookeeper.
    /// <summary>
    /// You can copy out code of this class, the code under namespace KafkaNET.Library.Examples maybe change without notification.
    /// </summary>
    public class KafkaNetLibraryExample
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(KafkaNetLibraryExample));

        public static void MainInternal(int taskIndexInStorm, string[] args)
        {
            GenerateAssistFile("producesimple");
            GenerateAssistFile("produceperftest");
            GenerateAssistFile("producemonitor");
            GenerateAssistFile("eventserverperftest");
            GenerateAssistFile("consumesimple");
            GenerateAssistFile("consumegroup");
            GenerateAssistFile("consumegroupmonitor");
            GenerateAssistFile("topic");
            ServicePointManager.DefaultConnectionLimit = 5000;
            ServicePointManager.UseNagleAlgorithm = false;

            var log4netSection = ConfigurationManager.GetSection("log4net");
            if (log4netSection != null)
            {
                XmlConfigurator.Configure();
            }

            KafkaNETExampleCommandVerb commandOptions = new KafkaNETExampleCommandVerb();
            try
            {
                commandOptions.Parse(args);
            }
            catch (Exception e)
            {
                Logger.ErrorFormat("{0}", e.FormatException());
                Console.WriteLine(KafkaNETExampleCommandVerb.GetUsage());
                Environment.Exit(-1);
            }

            KafkaNETExampleSubArguments realOption = KafkaNETExampleCommandVerb.ActiveSubOption;
            try
            {
                realOption.Parse(args);
            }
            catch (Exception e)
            {
                Logger.ErrorFormat("{0}", e.FormatException());
                Console.WriteLine(realOption.GetUsage(false));
                Environment.Exit(-1);
            }

            Logger.InfoFormat("All arguments of {0}: \r\n{1}", KafkaNETExampleCommandVerb.AssemblyName, realOption.GetArgDict());

            switch (KafkaNETExampleCommandVerb.Verb)
            {
                case "producesimple":
                case "produceroundrobin":
                    ProduceSimpleHelperOption produceroundrobinOptions = (ProduceSimpleHelperOption)realOption;
                    ProduceSimpleHelper.Run(produceroundrobinOptions);
                    break;
                case "produceperftest":
                case "producewrapper":
                    ProducePerfTestHelperOption producewrapperOption = (ProducePerfTestHelperOption)realOption;
                    (new ProducePerfTestHelper()).Run(producewrapperOption);
                    break;
                case "producem":
                case "producemonitor":
                    ProduceMonitorHelperOptions produceMonitorOptions = (ProduceMonitorHelperOptions)realOption;
                    ProduceMonitorHelper.Run(produceMonitorOptions);
                    break;
                case "eventserverperftest":
                    JavaEventServerPerfTestHelperOptions evetServerPerfTestOptions = (JavaEventServerPerfTestHelperOptions)realOption;
                    (new JavaEventServerPerfTestHelper()).Run(evetServerPerfTestOptions);
                    break;
                case "consumesimple":
                case "dumpdata":
                    ConsumeDataHelperArguments dumpdataOptions = (ConsumeDataHelperArguments)realOption;
                    ConsumeSimpleHelper.ConsumeDataSimple(dumpdataOptions);
                    break;
                case "consumegroup":
                case "dumpdataasconsumergroup":
                    ConsumeGroupHelperOptions cgOptions = (ConsumeGroupHelperOptions)realOption;
                    if (taskIndexInStorm >= 0)
                    {
                        cgOptions.ConsumerId = cgOptions.ConsumerId + taskIndexInStorm.ToString();
                        cgOptions.File = cgOptions.ConsumerId + taskIndexInStorm.ToString() + cgOptions.File;
                    }

                    ConsumerGroupHelper.DumpMessageAsConsumerGroup(cgOptions);
                    break;
                case "latestoffsetofconsumergroup":
                case "consumegroupmonitor":
                case "consumegroupm":
                case "consumem":
                    ConsumeGroupMonitorHelperOptions dcgOptions = (ConsumeGroupMonitorHelperOptions)realOption;
                    ConsumeGroupMonitorHelper.DumpConsumerGroupOffsets(dcgOptions);
                    break;
                case "topic":
                    TopicHelperArguments dtOptions = (TopicHelperArguments)realOption;
                    TopicHelper.DumpTopicMetadataAndOffset(dtOptions);
                    break;
                case "test":
                    var testOptions = (TestHelperOptions)realOption;
                    TestHelper.Run(testOptions);
                    break;
                default:
                    Logger.Error(string.Format("Invalid verb={0}", KafkaNETExampleCommandVerb.Verb));
                    return;
            }
        }

        private static void GenerateAssistFile(string p)
        {
            if (!File.Exists(p))
            {
                using (StreamWriter sw = new StreamWriter(p, false))
                {
                    sw.Write("For quick command ...");
                }
            }
        }

        internal static KafkaOffsetType ConvertOffsetType(string offset)
        {
            KafkaOffsetType offsetType = KafkaOffsetType.Earliest;

            if (string.IsNullOrEmpty(offset))
            {
                throw new ArgumentNullException("offset");
            }

            switch (offset.ToLower(CultureInfo.InvariantCulture))
            {
                case "earliest":
                    offsetType = KafkaOffsetType.Earliest;
                    break;
                case "latest":
                    offsetType = KafkaOffsetType.Latest;
                    break;
                case "last":
                    offsetType = KafkaOffsetType.Last;
                    break;
                default:
                    offsetType = KafkaOffsetType.Timestamp;
                    break;
            }

            return offsetType;
        }

        internal static long ConvertOffset(string offset)
        {
            long offsetTime = 0;
            bool success = false;

            if (string.IsNullOrEmpty(offset))
            {
                throw new ArgumentNullException("offset");
            }

            switch (offset.ToLower(CultureInfo.InvariantCulture))
            {
                case "earliest":
                case "latest":
                case "last":
                    offsetTime = 0;
                    break;
                default:
                    DateTime dateTimeOffset;
                    if (DateTime.TryParse(offset, out dateTimeOffset))
                    {
                        offsetTime = KafkaClientHelperUtils.ToUnixTimestampMillis(dateTimeOffset);
                        success = true;
                    }
                    else if (long.TryParse(offset, out offsetTime))
                    {
                        success = true;
                    }

                    if (!success)
                    {
                        Logger.Error(string.Format("Error: invalid offset={0}, it should be either earliest|latest|last or an unsigned integer or a timestamp.", offset));
                        throw new ArgumentException(string.Format("invalid offset={0}", offset));
                    }

                    break;
            }

            return offsetTime;
        }

        internal static CompressionCodecs ConvertToCodec(int c)
        {
            return ConvertToCodec(c.ToString());
        }

        internal static CompressionCodecs ConvertToCodec(string c)
        {
            if (string.IsNullOrEmpty(c)
                || c == "0")
                return CompressionCodecs.NoCompressionCodec;
            else if (c == "1")
                return CompressionCodecs.DefaultCompressionCodec;
            else if (c == "2")
                return CompressionCodecs.SnappyCompressionCodec;
            else
                return CompressionCodecs.NoCompressionCodec;
        }

        internal static void AppendLineToFile(string f, string s)
        {
            if (!string.IsNullOrEmpty(f))
            {
                using (StreamWriter sw = new StreamWriter(f, true))
                {
                    sw.WriteLine(s);
                }
            }
        }

        internal static void AppendToFile(string f, string s)
        {
            if (!string.IsNullOrEmpty(f))
            {
                using (StreamWriter sw = new StreamWriter(f, true))
                {
                    sw.Write(s);
                }
            }
        }
    }
}

/*
 
 * KafkaNETLibraryConsole.exe topic -z stcads033 -t mvlogs3
 * KafkaNETLibraryConsole.exe  producesimple -z stcads033 -t mvlogs3 -p -1
 * KafkaNETLibraryConsole.exe  producesimple -z stcads033 -t mvlogs3 -p 0
 * KafkaNETLibraryConsole.exe  producesimple -z stcads033 -t mvlogs3 -p 1
 * KafkaNETLibraryConsole.exe  producesimple -z stcads033 -t mvlogs3 -p 2
 * KafkaNETLibraryConsole.exe  producesimple -z stcads033 -t mvlogs3 -p -2 -l Kafka.Client.Producers.Partitioning.DefaultPartitioner`1 
 * KafkaNETLibraryConsole.exe  producesimple -z stcads033 -t mvlogs3 -p -2 -l KafkaNET.Library.Examples.CustomePartitionerSample`1 
 * KafkaNETLibraryConsole.exe  consumesimple -z stcads033 -t mvlogs3
 * KafkaNETLibraryConsole.exe  consumegroup -z stcads033 -g G1 -d C1 -t mvlogs3
 
 */
