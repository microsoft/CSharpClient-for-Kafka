using System;
using System.Collections.Generic;

namespace Kafka.Client.ZooKeeperIntegration.Events
{
    public class ConsumerRebalanceEventArgs : EventArgs
    {
        public IDictionary<string, ICollection<int>> TopicPartitionMap { get; private set; }

        public ConsumerRebalanceEventArgs(Dictionary<string, ICollection<int>> topicPartitionMap)
        {
            TopicPartitionMap = topicPartitionMap;
        }
    }
}