// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// The exception that is thrown when broker information is not available on ZooKeeper server
    /// </summary>
    public class BrokerNotAvailableException : Exception
    {
        public BrokerNotAvailableException(int brokerId)
            : base()
        {
            this.BrokerId = brokerId;
        }

        public BrokerNotAvailableException()
            : base()
        {
        }

        public BrokerNotAvailableException(string message)
            : base(message)
        {
        }

        public BrokerNotAvailableException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public int? BrokerId { get; set; }
    }
}
