// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Responses
{
    using Kafka.Client.Serialization;

    public interface IResponseParser<out T>
    {
        T ParseFrom(KafkaBinaryReader reader);
    }
}
