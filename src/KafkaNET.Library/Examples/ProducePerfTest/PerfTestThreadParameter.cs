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

    internal class PerfTestThreadParameter
    {
        internal AutoResetEvent EventOfFinish { get; set; }
        internal int ThreadId;
        internal int RequestIndex;
        internal int RequestSucc;
        internal int RequestFail;
        internal int RequestFailAfterRetry;
        internal long SentSuccBytes;
        internal Stopwatch StopWatchForSuccessRequest;
        internal double SuccRecordsTimeInms;
        internal double SpeedConstrolMBPerSecond = 0.0;
        internal string Topic;
        internal int PartitionId;
        internal bool RandomReturnIfNotExist;

        internal PerfTestThreadParameter()
        {
            this.StopWatchForSuccessRequest = Stopwatch.StartNew();
            this.StopWatchForSuccessRequest.Reset();
        }
    }
}
