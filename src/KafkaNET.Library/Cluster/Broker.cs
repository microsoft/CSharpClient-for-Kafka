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
    using Kafka.Client.Requests;
    using Kafka.Client.Serialization;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using System.Web.Script.Serialization;

    /// <summary>
    /// Represents Kafka broker
    /// </summary>
    public class Broker : IWritable
    {
        public const byte DefaultPortSize = 4;
        public const byte DefaultBrokerIdSize = 4;

        /// <summary>
        /// Initializes a new instance of the <see cref="Broker"/> class.
        /// </summary>
        /// <param name="id">
        /// The broker id.
        /// </param>
        /// <param name="host">
        /// The broker host.
        /// </param>
        /// <param name="port">
        /// The broker port.
        /// </param>
        public Broker(int id, string host, int port)
        {
            this.Id = id;
            this.Host = host;
            this.Port = port;
        }

        public static Broker CreateBroker(int id, string brokerInfoString)
        {
            if (string.IsNullOrEmpty(brokerInfoString))
            {
                throw new ArgumentException(string.Format("Broker id {0} does not exist", id));
            }

            var ser = new JavaScriptSerializer();
            var result = ser.Deserialize<Dictionary<string, object>>(brokerInfoString);
            var host = result["host"].ToString();
            return new Broker(id, host, int.Parse(result["port"].ToString(), CultureInfo.InvariantCulture));            
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder(1024);
            sb.AppendFormat("Broker.Id:{0},Host:{1},Port:{2}", this.Id, this.Host, this.Port);
            string s = sb.ToString();
            sb.Length = 0;
            return s;
        }

        /// <summary>
        /// Gets the broker Id.
        /// </summary>
        public int Id { get; private set; }

        /// <summary>
        /// Gets the broker host.
        /// </summary>
        public string Host { get; private set; }

        /// <summary>
        /// Gets the broker port.
        /// </summary>
        public int Port { get; private set; }

        public int SizeInBytes
        {
            get
            {
                return BitWorks.GetShortStringLength(this.Host, AbstractRequest.DefaultEncoding) +
                    DefaultPortSize + DefaultBrokerIdSize;
            }
        }

        public void WriteTo(MemoryStream output)
        {
            Guard.NotNull(output, "output");

            using (var writer = new KafkaBinaryWriter(output))
            {
                this.WriteTo(writer);
            }
        }

        public void WriteTo(KafkaBinaryWriter writer)
        {
            Guard.NotNull(writer, "writer");

            writer.Write(this.Id);
            writer.WriteShortString(this.Host, AbstractRequest.DefaultEncoding);
            writer.Write(this.Port);
        }

        internal static Broker ParseFrom(KafkaBinaryReader reader)
        {
            var id = reader.ReadInt32();
            var host = BitWorks.ReadShortString(reader, AbstractRequest.DefaultEncoding);
            var port = reader.ReadInt32();
            return new Broker(id, host, port);
        }
    }
}
