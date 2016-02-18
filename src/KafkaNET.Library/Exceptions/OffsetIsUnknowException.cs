// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Exceptions
{
    using System;

    /// <summary>
    /// The exception that is thrown when could not retrieve offset from broker for specific partition of specific topic
    /// </summary>
    public class OffsetIsUnknowException : Exception
    {
        public OffsetIsUnknowException(string topic, int brokerId, int partitionId)
            : base()
        {
            this.BrokerId = brokerId;
            this.PartitionId = partitionId;
            this.Topic = topic;
        }

        public OffsetIsUnknowException()
            : base()
        {
        }

        public OffsetIsUnknowException(string message)
            : base(message)
        {
        }

        public int? BrokerId { get; set; }

        public int? PartitionId { get; set; }

        public string Topic { get; set; }
    }
}

