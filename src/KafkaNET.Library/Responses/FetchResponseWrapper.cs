// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Responses
{
    using Kafka.Client.Messages;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    public class FetchResponseWrapper
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(FetchResponseWrapper));

        public FetchResponseWrapper(List<MessageAndOffset> list, int size, int correlationid, string topic, int partitionId)
        {
            this.MessageAndOffsets = list;
            this.Size = size;
            this.CorrelationId = correlationid;
            this.Topic = topic;
            this.PartitionId = partitionId;
            this.MessageCount = this.MessageAndOffsets.Count;
        }

        public int Size { get; private set; }
        public int CorrelationId { get; private set; }
        public List<MessageAndOffset> MessageAndOffsets { get; private set; }
        public int MessageCount { get; set; }
        public string Topic { get; private set; }
        public int PartitionId { get; private set; }

    }
}
