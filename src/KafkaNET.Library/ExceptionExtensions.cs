// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public static class ExceptionExtensions
    {
        /// <summary>
        ///     Formats an exception object into a printable string.
        /// </summary>
        /// <param name="exception">the exception to format</param>
        /// <returns>a string</returns>
        public static string FormatException(this Exception exception)
        {
            if (exception == null)
            {
                return string.Empty;
            }

            var output = new StringBuilder();

            Exception currentException = exception;
            while (currentException != null)
            {
                output.AppendFormat("Exception Message: {0}\r\n", currentException.Message);
                output.AppendFormat("Source: {0}\r\n", currentException.Source);
                output.AppendFormat("Stack Trace:\r\n {0}\r\n", currentException.StackTrace);
                currentException = currentException.InnerException;
                if (currentException != null)
                {
                    output.Append("\r\n---- Inner Exception ----\r\n");
                }
            }

            return output.ToString();
        }
    }
}
