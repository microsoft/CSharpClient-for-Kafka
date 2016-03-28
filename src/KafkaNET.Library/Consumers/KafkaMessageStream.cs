/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Kafka.Client.Consumers
{
    using Kafka.Client.Messages;
    using Kafka.Client.Serialization;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// This class is a thread-safe IEnumerable of <see cref="Message"/> that can be enumerated to get messages.
    /// </summary>
    public class KafkaMessageStream<TData> : IKafkaMessageStream<TData>
    {
        private readonly BlockingCollection<FetchedDataChunk> queue;

        private readonly int consumerTimeoutMs;

        public IConsumerIterator<TData> iterator { get; private set; }

        private string topic;

        private IDecoder<TData> decoder;

        internal KafkaMessageStream(string topic, BlockingCollection<FetchedDataChunk> queue, int consumerTimeoutMs, IDecoder<TData> decoder)
        {
            this.topic = topic;
            this.consumerTimeoutMs = consumerTimeoutMs;
            this.queue = queue;
            this.decoder = decoder;
            this.iterator = new ConsumerIterator<TData>(this.topic, this.queue, this.consumerTimeoutMs, this.decoder);
        }

        internal KafkaMessageStream(string topic, BlockingCollection<FetchedDataChunk> queue, int consumerTimeoutMs, IDecoder<TData> decoder, CancellationToken token)
        {
            this.topic = topic;
            this.consumerTimeoutMs = consumerTimeoutMs;
            this.queue = queue;
            this.decoder = decoder;
            this.iterator = new ConsumerIterator<TData>(topic, queue, consumerTimeoutMs, decoder, token);
        }

        public IKafkaMessageStream<TData> GetCancellable(CancellationToken cancellationToken)
        {
            return new KafkaMessageStream<TData>(this.topic, this.queue, this.consumerTimeoutMs, this.decoder, cancellationToken);
        }

        public int Count
        {
            get
            {
                return queue.Count;
            }
        }
        public void Clear()
        {
            iterator.ClearIterator();
        }

        public IEnumerator<TData> GetEnumerator()
        {
            return this.iterator;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
