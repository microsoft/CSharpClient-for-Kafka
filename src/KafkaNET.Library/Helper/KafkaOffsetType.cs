// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public enum KafkaOffsetType
    {
        Earliest = 0,
        Current = 1,
        Last = 2,
        Latest = 3,
        Timestamp = 4
    }
}
