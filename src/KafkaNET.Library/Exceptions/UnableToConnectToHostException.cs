// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    public class UnableToConnectToHostException : Exception
    {
        public UnableToConnectToHostException(string server, int port)
            : base(string.Format("Unable to connect to {0}:{1}", server, port))
        {
        }

        public UnableToConnectToHostException(string server, int port, Exception innterException)
            : base(string.Format("Unable to connect to {0}:{1}", server, port), innterException)
        {
        }

        public UnableToConnectToHostException() : base()
        {
        }
        public UnableToConnectToHostException(string message, Exception innterException)
            : base(message, innterException)
        {
        }
        public UnableToConnectToHostException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }
}
