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

namespace Kafka.Client.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    /// <summary>
    /// Represents broker partition
    /// </summary>
    public class Partition
    {
        /// <summary>
        /// Gets the partition ID.
        /// </summary>
        public int PartId { get; private set; }

        public string Topic { get; private set; }

        public Replica Leader { get; set; }

        public HashSet<Replica> AssignedReplicas { get; private set; }

        public HashSet<Replica> InSyncReplicas { get; private set; }

        public HashSet<Replica> CatchUpReplicas { get; private set; }

        public HashSet<Replica> ReassignedReplicas { get; private set; }

        public Partition(string topic, int partId, Replica leader = null, HashSet<Replica> assignedReplicas = null,
            HashSet<Replica> inSyncReplica = null, HashSet<Replica> catchUpReplicas = null, HashSet<Replica> reassignedReplicas = null)
        {
            this.Topic = topic;
            this.Leader = leader;
            this.PartId = partId;
            this.AssignedReplicas = assignedReplicas ?? new HashSet<Replica>();
            this.InSyncReplicas = inSyncReplica ?? new HashSet<Replica>();
            this.CatchUpReplicas = catchUpReplicas ?? new HashSet<Replica>();
            this.ReassignedReplicas = reassignedReplicas ?? new HashSet<Replica>();
        }
        public override string ToString()
        {
            return string.Format("Topic={0},PartId={1},LeaderBrokerId={2}"
                , this.Topic
                , this.PartId
                , this.Leader==null?"NA":this.Leader.BrokerId.ToString());
        }
    }
}
