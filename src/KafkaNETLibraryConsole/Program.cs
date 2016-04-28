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
            KafkaNetLibraryExample.MainInternal(-1, args);
        }  
    }
}
