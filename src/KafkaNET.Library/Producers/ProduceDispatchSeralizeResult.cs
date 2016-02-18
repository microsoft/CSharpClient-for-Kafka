// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Producers
{
    using Kafka.Client.Messages;
    using Kafka.Client.Responses;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public class ProduceDispatchSeralizeResult<K>
    {
        public ProduceDispatchSeralizeResult(IEnumerable<Exception> exceptions,
            IEnumerable<ProducerData<K, Message>> failedProducerDatas, List<Tuple<int, TopicAndPartition, ProducerResponseStatus>> failedDetail, bool hasDataNeedDispatch)
        {
            Exceptions = exceptions;
            FailedProducerDatas = failedProducerDatas;
            FailedDetail = failedDetail;
            this.HasDataNeedDispatch = hasDataNeedDispatch;
        }

        public IEnumerable<ProducerData<K, Message>> FailedProducerDatas { get; private set; }
        public IEnumerable<Exception> Exceptions { get; private set; }
        public List<Tuple<int, TopicAndPartition, ProducerResponseStatus>> FailedDetail { get; private set; }
        public bool HasDataNeedDispatch { get; private set; }
    }
}
