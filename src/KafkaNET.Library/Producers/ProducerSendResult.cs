// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Producers
{
    using System;

    public class ProducerSendResult<TReturn>
    {
        public ProducerSendResult(TReturn returnVal)
        {
            ReturnVal = returnVal;
            Success = true;
        }

        public ProducerSendResult(Exception e)
        {
            Exception = e;
            Success = false;
        }

        public ProducerSendResult(TReturn returnVal, Exception e)
        {
            ReturnVal = returnVal;
            Exception = e;
            Success = false;
        }

        public TReturn ReturnVal { get; private set; }
        public Exception Exception { get; private set; }
        public bool Success { get; private set; }
    }
}
