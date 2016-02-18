// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    public class BadMessageSizeFromKafkaException : Exception
    {
        public BadMessageSizeFromKafkaException(int expected, int found) :
            base(string.Format("Message size expected to be {0} but read {1}", expected, found)) { }

        public BadMessageSizeFromKafkaException(SerializationInfo info, StreamingContext context)
            : base(info, context) 
        { 

        }

        public BadMessageSizeFromKafkaException() 
        { 

        }
    }
}
