// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Helper;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class PerfTestHelperBase
    {
        internal static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(PerfTestHelperBase));

        protected static Stopwatch stopWatch = new Stopwatch();
        protected static ProducePerfTestHelperOption produceOption = null;
        protected static PerfTestThreadParameter[] threadPareamaters = null;

        protected void Statistics()
        {
            TimeSpan ts = stopWatch.Elapsed;
            // Format and display the TimeSpan value. 
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds / 10);
            long succBatch = threadPareamaters.Sum(r => r.RequestSucc);
            long succ = threadPareamaters.Sum(r => r.RequestSucc) * produceOption.MessageCountPerBatch;
            long fail = threadPareamaters.Sum(r => r.RequestFail) * produceOption.MessageCountPerBatch;
            long failAfterRetry = threadPareamaters.Sum(r => r.RequestFailAfterRetry) * produceOption.MessageCountPerBatch;
            long all = threadPareamaters.Sum(r => r.RequestIndex) * produceOption.MessageCountPerBatch;
            StringBuilder sbFailRate = new StringBuilder();
            sbFailRate.AppendFormat("FailRate:{0:P2}   Succ:{1} Fail:{2}", (1.0 * failAfterRetry) / all, succ, failAfterRetry);

            double mbTransferred = succ * (produceOption.MessageSize / 1048576.0);
            long totalMessageCount = succ;
            StringBuilder sbSummary = new StringBuilder();
            double averageLatency = threadPareamaters.Sum(r => r.SuccRecordsTimeInms) / succBatch;
            sbSummary.AppendFormat("Total: {0:00} s, {1:0.00} MB, {2:0.00} MB/s, {3:0.00} message/s. Totally {4} messsages. FailRate:{5:P2}   Fail:{6} BatchLatency:{7}ms",
                ts.TotalSeconds, mbTransferred, mbTransferred / ts.TotalSeconds, ((double)totalMessageCount) / ts.TotalSeconds, totalMessageCount
                , (1.0 * failAfterRetry) / all, failAfterRetry, averageLatency);
            Logger.Info(sbSummary.ToString());
        }
    }
}
