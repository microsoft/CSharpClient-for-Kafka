// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Microsoft.KafkaNET.Library.Util
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public class ExceptionUtil
    {
        public static string GetExceptionDetailInfo(Exception exception, bool flattenAggregateException = true)
        {
            var builder = new StringBuilder();

            if (exception != null)
            {
                builder.Append(Environment.NewLine);

                using (var writer = new StringWriter(builder))
                {
                    (new TextExceptionFormatter(writer, exception)
                    {
                        FlattenAggregateException = flattenAggregateException
                    }).Format();
                }
            }
            return builder.ToString();
        }

    }
}
