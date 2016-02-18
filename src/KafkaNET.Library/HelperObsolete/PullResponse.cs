
namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Kafka.Client.Cfg;
    using Kafka.Client.Cluster;
    using Kafka.Client.Consumers;
    using Kafka.Client.Messages;
    using Kafka.Client.Producers;
    using Kafka.Client.Producers.Sync;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;

    /// <summary>
    /// Obsolete class
    /// Please use PartitionData.GetMessageAndOffsets
    /// </summary>
    public class PullResponse
    {
        public PullResponse(PartitionData pData)
        {
            this.Partition = pData.Partition;
            this.Error = pData.Error;
          
            this.Payload = new List<byte[]>();
            foreach (MessageAndOffset m in pData.MessageSet)
            {
                this.Payload.Add(m.Message.Payload);                  
            }
        }

        public int Partition { get; private set; }

        public ErrorMapping Error { get; private set; }
        public IList<byte[]> Payload { get; private set; }
    }
    /// <summary>
    /// Obsolete class
    /// Please use PartitionData.GetMessageAndOffsets
    /// </summary>
    public class PullResponseExt
    {
        public PullResponseExt(PartitionData pData)
        {
            this.Partition = pData.Partition;
            this.Error = pData.Error;

            this.OffsetsAndPayLoads = new List<Tuple<long, byte[]>>();

            foreach (MessageAndOffset m in pData.MessageSet)
            {
                this.OffsetsAndPayLoads.Add( new Tuple<long,byte[]>(m.Message.Offset,  m.Message.Payload));
            }
        }

        public int Partition { get; private set; }

        public ErrorMapping Error { get; private set; }
        public IList<Tuple<long,byte[]>> OffsetsAndPayLoads { get; private set; }
    }
}
