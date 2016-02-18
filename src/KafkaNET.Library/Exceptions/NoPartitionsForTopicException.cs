// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Exceptions
{
    using System;

    /// <summary>
    /// The exception that is thrown when no partitions found for specified topic
    /// </summary>
    public class NoPartitionsForTopicException : Exception
    {
        public NoPartitionsForTopicException(string topic)
            : base()
        {
            this.Topic = topic;
        }

        public NoPartitionsForTopicException()
            : base()
        {
        }

        public NoPartitionsForTopicException(string topic, string message)
            : base(message)
        {
            this.Topic = topic;
        }

        public string Topic { get; set; }
    }
}

