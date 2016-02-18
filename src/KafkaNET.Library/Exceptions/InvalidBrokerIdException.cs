// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    public class InvalidBrokerIdException : Exception
    {
        public InvalidBrokerIdException()
            : base()
        {
        }

        public InvalidBrokerIdException(string message)
            : base(message)
        {
        }

        public InvalidBrokerIdException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
