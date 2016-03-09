// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Threading;
using Kafka.Client.Cfg;
using Kafka.Client.Consumers;
using Kafka.Client.Helper;
using Kafka.Client.Messages;
using Kafka.Client.Producers;
using Kafka.Client.Requests;

namespace KafkaNET.Library.Examples
{
    class Program
    {
        private const string PhotonMessageOutgoingTopic = "photon_enet_message_outgoing-photon_id";

        static void Main(string[] args)
        {           
            //KafkaNetLibraryExample.MainInternal(-1, args);

            var managerConfig = new KafkaSimpleManagerConfiguration()
            {
                Zookeeper = "127.0.0.1:2181"
            };

            var consumerManager = new KafkaSimpleManager<byte[], Message>(managerConfig);
            long earliestOffset;
            long offset = 15000;

            var clientId = "photon-" + Environment.MachineName;

            consumerManager.RefreshMetadata(1, clientId, 0, PhotonMessageOutgoingTopic, true);
            consumerManager.RefreshAndGetOffset(1, clientId, 1, PhotonMessageOutgoingTopic, 0, true,
                out earliestOffset, out offset);
            var consumer = consumerManager.GetConsumer(PhotonMessageOutgoingTopic, 0);

            int corrId = 10;

            while (true)
            {
                corrId++;
                var requestMap = new Dictionary<string, List<PartitionFetchInfo>>();
                var fetchInfo = new PartitionFetchInfo(0, offset, 1024 * 1024);
                requestMap[PhotonMessageOutgoingTopic] = new List<PartitionFetchInfo>(new[] {fetchInfo});
                var fetchRequest = new FetchRequest(corrId, clientId, 30000, 1, requestMap);
                var readMessages = false;

                try
                {
                    var fetchResponse = consumer.Fetch(clientId, PhotonMessageOutgoingTopic, corrId++, 0, offset, 1024 * 1024, 30000, 1);
                    TopicData topicData;

                    if (fetchResponse.TopicDataDict.TryGetValue(PhotonMessageOutgoingTopic, out topicData))
                    {

                        foreach (var partitionData in topicData.PartitionData)
                        {
                            foreach (var message in partitionData.MessageSet.Messages)
                            {
                                readMessages = true;
                                offset++;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                }
            }
        }  
    }
}
