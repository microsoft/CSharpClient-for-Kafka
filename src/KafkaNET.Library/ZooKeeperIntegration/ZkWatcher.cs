using System;
using System.Collections.Generic;
using ZooKeeperNet;

namespace Kafka.Client.ZooKeeperIntegration
{
    class ZkWatcher : IWatcher
    {
        public void Process(WatchedEventArgs @event)
        {
            // no-op
        }
    }
}
