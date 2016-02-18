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

    public class TimeStampTooSmallException : Exception
    {
        private long offsetTime;


        public TimeStampTooSmallException()
            : base()
        {
        }

        public TimeStampTooSmallException(string message)
            : base(message)
        {
        }

        public TimeStampTooSmallException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public TimeStampTooSmallException(long offsetTime)
        {
            this.offsetTime = offsetTime;
        }
    }
}
