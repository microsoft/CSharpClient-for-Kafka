// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Exceptions
{
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;

    public class KafkaClientException : Exception
    {
        public ErrorMapping ErrorCode { get; set; }

        public KafkaClientException()
            : base()
        {
        }

        public KafkaClientException(string message) : base(message)
        {
        }

        public KafkaClientException(string message, ErrorMapping errorCode) : this(message)
        {
            this.ErrorCode = errorCode;
        }

        public KafkaClientException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
