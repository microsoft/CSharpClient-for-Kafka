namespace Kafka.Client.Helper
{
    using Kafka.Client.Cfg;
    using Kafka.Client.Consumers;
    using Kafka.Client.Requests;
    using Kafka.Client.Utils;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Temporaly add back for VERITAS
    /// This class should be obsolete.
    /// Please use KafkaSimpleManger and KafkaSimpleManagerConfiguration    
    /// </summary>
    [CLSCompliant(false)]
    public class ConsumerHelper
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(ConsumerHelper));

        public static PullResponse PullMessage(
            ConsumerConfiguration consumerConfig,
            BrokerConfiguration leaderBrokerConfig,
            int correlationID,
            string topic,
            int partitionIndex,
            long fetchOffset,
            KafkaClientHelperConfiguration helperConfiguration,
            out long offsetNew)
        {
            offsetNew = -1;
            PullResponse result = null;
            int payloadCount = 0;

            // at least retry once
            int maxRetry = 1;
            int retryCount = 0;
            string s = string.Empty;
            bool success = false;
            while (!success && retryCount < maxRetry)
            {
                try
                {
                    var requestMap = new Dictionary<string, List<PartitionFetchInfo>>();
                    requestMap.Add(
                        topic,
                        new List<PartitionFetchInfo>()
                        {
                            new PartitionFetchInfo(
                                partitionIndex,
                                fetchOffset,
                                helperConfiguration.FetchSize)
                        });
                    using (Consumer consumer = new Consumer(consumerConfig, leaderBrokerConfig.Host, leaderBrokerConfig.Port))
                    {
                        var response = consumer.Fetch(new FetchRequest(
                            correlationID, //random.Next(int.MinValue, int.MaxValue),                        // correlation id
                            Assembly.GetExecutingAssembly().ManifestModule.ToString(),      // client id
                            helperConfiguration.MaxWaitTime,
                            helperConfiguration.MinWaitBytes,
                            requestMap));

                        if (response == null)
                        {
                            throw new KeyNotFoundException(string.Format("FetchRequest returned null response,fetchOffset={0},leader={1},topic={2},partition={3}",
                                fetchOffset, leaderBrokerConfig, topic, partitionIndex));
                        }

                        var partitionData = response.PartitionData(topic, partitionIndex);
                        if (partitionData == null)
                        {
                            throw new KeyNotFoundException(string.Format("PartitionData is null,fetchOffset={0},leader={1},topic={2},partition={3}",
                                fetchOffset, leaderBrokerConfig, topic, partitionIndex));
                        }

                        if (partitionData.Error == ErrorMapping.OffsetOutOfRangeCode)
                        {
                            s = "PullMessage OffsetOutOfRangeCode,change to Latest,topic={0},leader={1},partition={2},FetchOffset={3},retryCount={4},maxRetry={5}";
                            Logger.ErrorFormat(s, topic, leaderBrokerConfig, partitionIndex, fetchOffset, retryCount, maxRetry);
                            return null;
                        }

                        if (partitionData.Error != ErrorMapping.NoError)
                        {
                            s = "PullMessage ErrorCode={0},topic={1},leader={2},partition={3},FetchOffset={4},retryCount={5},maxRetry={6}";
                            Logger.ErrorFormat(s, partitionData.Error, topic, leaderBrokerConfig, partitionIndex, fetchOffset, retryCount, maxRetry);
                            return null;
                        }

                        var messages = partitionData.MessageSet.Messages;

                        s = "PullMessage AfterFetch,resultMessageCount={0},topic={1},leader={2},partition={3},FetchOffset={4},retryCount={5},maxRetry={6}";
                        Logger.DebugFormat(s, null == messages ? "(null)" : messages.Count().ToString(), topic, leaderBrokerConfig, partitionIndex, fetchOffset, retryCount, maxRetry);

                        success = true;
                        result = new PullResponse(partitionData);

                        if (null != messages && messages.Count() > 0)
                        {
                            payloadCount = messages.Count();
                            long lastOffset = messages.Last().Offset;

                            if ((payloadCount + fetchOffset) != (lastOffset + 1))
                            {
                                s = "PullMessage offset payloadCount out-of-sync,topic={0},leader={1},partition={2},payloadCount={3},FetchOffset={4},lastOffset={5},retryCount={6},maxRetry={7}";
                                Logger.ErrorFormat(s, topic, leaderBrokerConfig, partitionIndex, payloadCount, fetchOffset, lastOffset, retryCount, maxRetry);
                            }
                            offsetNew = messages.Last().Offset + 1;
                        }

                        return result;
                    }
                }
                catch (Exception)
                {
                    if (retryCount >= maxRetry)
                    {
                        throw;
                    }
                }
                finally
                {
                    retryCount++;
                }
            } // end of while loop

            return result;
        }
    }
}
