// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Utils
{
    using Kafka.Client.Messages;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// This class mainly for KafkaNETLibraryConsole, maybe will change even break without any notification.
    /// </summary>
    public class KafkaConsoleUtil
    {
        public const int BrokerIDisIP = 256 * 256 * 256;
        public static string GetBrokerIDAndIP(int brokerId)
        {
            //The IP at lease 1.0.0.0
            if (brokerId > BrokerIDisIP)
            {
                return string.Format("{0}.{1}.{2}.{3}({4})"
                    , brokerId / 256 / 256 / 256
                    , (brokerId / 256 / 256) % 256
                    , (brokerId / 256) % 256
                    , brokerId % 256
                   , brokerId);
            }
            else
                return brokerId.ToString();//TODO: actually can get service.properties from all machine and cache
        }

        public static void DumpDataToFile(bool dumpDataAsUTF8, bool dumpOriginalData, StreamWriter sw, FileStream fs
            , List<MessageAndOffset> payload, long count, long offsetBase, ref int totalCountUTF8, ref int totalCountOriginal)
        {
            int i = 0;
            if (dumpDataAsUTF8)
            {
                sw.WriteLine("====UTF8====");
                i = 0;
                foreach (var v in payload)
                {
                    string text = Encoding.UTF8.GetString(v.Message.Payload);
                    string textKey = string.Empty;

                    if (v.Message.Key != null)
                    {
                        textKey = Encoding.UTF8.GetString(v.Message.Key);
                    }

                    sw.WriteLine("\tpayload[{0}],totalPayload={1}, OFFSET: {2} byteArraySize={3},key={4} text=\r\n{5}",
                         offsetBase + i, payload.Count, v.MessageOffset, v.Message.Payload.Length, textKey, text);
                    i++;
                    totalCountUTF8++;
                    if (totalCountUTF8 >= count && count > 0)
                        break;
                }
            }

            if (dumpOriginalData)
            {
                sw.WriteLine("====RAW====");
                i = 0;
                foreach (var v in payload)
                {
                    sw.WriteLine("\tpayload[{0}],totalPayload={1}, OFFSET: {2} byteArraySize={3}\r\n==Binary start==",
                         offsetBase + i, payload.Count, v.MessageOffset, v.Message.Payload.Length);
                    sw.Flush();
                    fs.Write(v.Message.Payload, 0, v.Message.Payload.Length);
                    fs.Flush();
                    sw.WriteLine("\r\n==Binary END==",
                        offsetBase + i, payload.Count, v.Message.Payload.Length);
                    i++;
                    totalCountOriginal++;
                    if (totalCountOriginal >= count && count > 0)
                        break;
                }

            }
        }
    }
}
