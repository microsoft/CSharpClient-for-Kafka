// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class KafkaNETExampleConstants
    {
        internal const int DefaultMessageCountPerBatch = 100;
        internal const int DefaultMessageSize = 4096;
        internal const int DefaultRequiredAcks = 1;
        internal const int DefaultProduceThreadCount = 16;
        internal const int DefaultIntefvalInSeconds = 10;
        internal const int DefaultRefreshConsumeGroupIntervalInSeconds = 600;
        internal const int DefaultCommitBatchSize = 4000;
        internal const int DefaultMaxFetchBufferLength = 10000;
        internal const int DefaultCancellationTimeoutMs = -1;
        internal const int DefaultProduceSleepMsOnceException = 5000;
        internal const int DefaultVersionId = 0;
        internal const int DefaultProducePartitionId = -1;
    }
}
