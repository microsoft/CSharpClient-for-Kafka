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

    public class KafkaConsumeException : KafkaClientException
    {
         public KafkaConsumeException()
            : base()
        {
        }

        public KafkaConsumeException(string message):base(message)
        {
        }

        public KafkaConsumeException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public KafkaConsumeException(string message, ErrorMapping errorCode)
            : base(message, errorCode)
        {
        }
    }
}
