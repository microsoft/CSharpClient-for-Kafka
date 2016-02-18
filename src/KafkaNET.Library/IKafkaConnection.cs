// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client
{
    using System;
    using System.Collections.Generic;
    using Kafka.Client.Consumers;
    using Kafka.Client.Producers;
    using Kafka.Client.Requests;
    using Kafka.Client.Responses;

    public interface IKafkaConnection : IDisposable
    {
        FetchResponse Send(FetchRequest request);
        ProducerResponse Send(ProducerRequest request);
        OffsetResponse Send(OffsetRequest request);
        IEnumerable<TopicMetadata> Send(TopicMetadataRequest request);
    }
}
