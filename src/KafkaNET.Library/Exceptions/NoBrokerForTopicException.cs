// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Exceptions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;

    public class NoBrokerForTopicException : KafkaClientException
    {
        public NoBrokerForTopicException()
           : base()
        {
        }

        public NoBrokerForTopicException(string message) : base(message)
        {
        }

        public NoBrokerForTopicException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
