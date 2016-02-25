// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Consumers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Kafka.Client.Cfg;
    using Kafka.Client.Producers.Sync;

    public class KafkaClientHelperUtils
    {
        private static log4net.ILog Logger { get { return log4net.LogManager.GetLogger(typeof(KafkaClientHelperUtils)); } }
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long GetCurrentUnixTimestampMillis()
        {
            return (long)(DateTime.UtcNow - UnixEpoch).TotalMilliseconds;
        }

        public static DateTime DateTimeFromUnixTimestampMillis(long millis)
        {
            return UnixEpoch.AddMilliseconds(millis);
        }

        /// <summary>
        /// The offset time in kafka is UTC
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public static long ToUnixTimestampMillis(DateTime time)
        {
            DateTime t = DateTime.SpecifyKind(time, DateTimeKind.Utc);
            return (long)(t.ToUniversalTime() - UnixEpoch).TotalMilliseconds;
        }

        /// <summary>
        /// Convert Zookeeper string to ZookeeperConfiguration
        /// </summary>
        /// <param name="zookeeperAddress"></param>
        /// <returns></returns>
        public static ZooKeeperConfiguration ToZookeeperConfig(string zookeeperAddress)
        {
            ZooKeeperConfiguration zookeeperConfig = null;
            if (!string.IsNullOrEmpty(zookeeperAddress))
            {
                zookeeperConfig = new ZooKeeperConfiguration();
                zookeeperConfig.ZkConnect = zookeeperAddress;
            }

            return zookeeperConfig;
        }

        /// <summary>
        /// Convert a broker string into a BrokerConfiguration.
        /// </summary>
        /// <param name="brokerAddr">Broker string must follow the format of {Broker-ID},{Host}:{Port}</param>
        /// <returns></returns>
        public static BrokerConfiguration ToBrokerConfig(string brokerAddr)
        {
            if (string.IsNullOrEmpty(brokerAddr))
            {
                return null;
            }

            string[] brokerParams = brokerAddr.Split(',');
            if (brokerParams == null || brokerParams.Length != 2)
            {
                return null;
            }

            int brokerId = -1;
            if (!int.TryParse(brokerParams[0], out brokerId))
            {
                return null;
            }

            string[] hostParams = brokerParams[1].Split(':');
            if (hostParams == null || hostParams.Length != 2)
            {
                return null;
            }

            int portNumber = -1;
            if (!int.TryParse(hostParams[1], out portNumber))
            {
                return null;
            }

            var broker = new BrokerConfiguration() { BrokerId = brokerId, Host = hostParams[0], Port = portNumber };
            return broker;
        }

        public static string ToBrokerString(int brokerId, string host, int port)
        {
            return string.Format("{0},{1}:{2}", brokerId, host, port); 
        }

        public static string SyncProducerConfigToString(SyncProducerConfiguration syncConfig)
        {
            if (syncConfig == null)
            {
                return "(SyncProducerConfiguration is null)";
            }
            return string.Format("BrokerId={0},Host={1}:Port={2}", syncConfig.BrokerId, syncConfig.Host, syncConfig.Port); 
        }

        /// <summary>
        /// SyncProducer makes a connection to the broker inside constructor
        /// </summary>
        /// <param name="brokerId"></param>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <returns></returns>
        public static SyncProducer TryCreateSyncProducer(int brokerId, string host, int port)
        {
            SyncProducer syncProducer = null;

            if (string.IsNullOrEmpty(host) || port <= 0)
            {
                Logger.ErrorFormat("TryCreateSyncProducer invalid arguments,host=[{0}] must not be empty or port=[{1}] must be valid", host, port);
                return null;
            }

            try
            {
                syncProducer = new SyncProducer(new SyncProducerConfiguration() { BrokerId = brokerId, Host = host, Port = port });
            }
            catch (Exception e)
            {
                Logger.Error(string.Format("TryCreateSyncProducer exception,brokerId={0},host={1},port={2}", brokerId, host, port), e);
                syncProducer = null;
            }

            return syncProducer;
        }

        public static string GetRandomString(int lengh)
        {
            if (lengh <= 0)
                return string.Empty;
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            var stringChars = new char[lengh];
            var random = new Random();

            for (int i = 0; i < stringChars.Length; i++)
            {
                stringChars[i] = chars[random.Next(chars.Length)];
            }

            var finalString = new String(stringChars);
            return finalString;
        }
        public static long GetValidStartReadOffset(KafkaOffsetType offsetType, long earliest, long latest, long offsetTimeStamp, int leastMessageCount)
        {
            long offset = 0; ;
            switch (offsetType)
            {
                case KafkaOffsetType.Earliest:
                    offset = earliest;
                    break;
                case KafkaOffsetType.Last:
                case KafkaOffsetType.Latest:
                    offset = latest;
                    break;
                case KafkaOffsetType.Timestamp:
                    offset = offsetTimeStamp;
                    break;
                default:
                    Logger.ErrorFormat("invalid offsetType={0}", offsetType);
                    throw new ArgumentOutOfRangeException(string.Format("offsetType={0}", offsetType));
            }
            if (offsetType == KafkaOffsetType.Timestamp)
            {
                if (offset == latest)
                {
                    offset -= leastMessageCount;
                    if (offset < earliest)
                    {
                        offset = earliest;
                    }
                }
            }
            else if (offsetType == KafkaOffsetType.Last
                || offsetType == KafkaOffsetType.Latest)
            {
                offset -= leastMessageCount;
                if (offset < earliest)
                {
                    offset = earliest;
                }
            }
            return offset;
        }
    }
}
