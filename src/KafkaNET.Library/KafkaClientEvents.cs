//using System.Diagnostics.CodeAnalysis;
//using System.Diagnostics.Tracing;


//namespace Kafka.Client.Logging
//{
//    [EventSource(Name = "Kafka-Client", Guid = GuidString)]
//    [ExcludeFromCodeCoverage]
//    public class KafkaClientEvents : EventSource
//    {
//        private const string GuidString = "{d0959b20-c9b9-4cbb-ad29-bfc683181867}";

//        private KafkaClientEvents() {}

//        private static readonly KafkaClientEvents Instance = new KafkaClientEvents();

//        public static KafkaClientEvents Logger
//        {
//            get { return Instance; }
//        }

//        [NonEvent]
//        public void DebugFormat(string message, params object[] objs)
//        {
//            if (IsEnabled())
//            {
//                Debug(string.Format(message, objs));
//            }
//        }

//        [Event(1, Level = EventLevel.Verbose, Message = "{0}")]
//        public void Debug(string message)
//        {
//            WriteEvent(1, message);
//        }

//        [Event(2, Level = EventLevel.Informational, Message = "Fetching metadata for topic {0}")]
//        public void UpdatingMetadataWithTopicMiss(string topic)
//        {
//            WriteEvent(2, topic);
//        }

//        [Event(3, Level = EventLevel.Error, Message = "Error while writing to compression stream. Type: {0}, Error: {1}")]
//        public void FailedWritingCompressionStream(string compressionType, string exception)
//        {
//            WriteEvent(3, compressionType, exception);
//        }

//        [Event(4, Level = EventLevel.Warning, Message = "Closing existing connection on reconnect received exception: {0}")]
//        public void FailedToCloseExistingConnectionOnReconnect(string exception)
//        {
//            WriteEvent(4, exception);
//        }

//        [Event(5, Level = EventLevel.Informational, Message = "Fetched metadata for topic: {0}")]
//        public void FetchingMetadataForTopic(string topic)
//        {
//            WriteEvent(5, topic);
//        }

//        [Event(6, Level = EventLevel.Informational, Message = "No sync producers available. Refreshing the available broker list from ZK and creating sync producers")]
//        public void RefreshingBrokersWithNoProducerFound()
//        {
//            WriteEvent(6);
//        }

//        [Event(7, Level = EventLevel.Error, Message = "Failed to send requests for topics {0}")]
//        public void FailedToSendMessages(string topics)
//        {
//            WriteEvent(7, topics);
//        }

//        [Event(8, Level = EventLevel.Warning, Message = "Unable to fetch topic info from broker {0}. Trying next broker. Reason: {1}")]
//        public void UnableToFetchTopicInfo(int brokerId, string error)
//        {
//            WriteEvent(8, brokerId, error);
//        }

//        [Event(9, Level = EventLevel.Warning, Message = "Failed to fetch messages. Error: {0}, Try count: {1}")]
//        public void FailedToFetchMessages(string exception, short tryCounter)
//        {
//            WriteEvent(9, exception, tryCounter);
//        }

//        [Event(10, Level = EventLevel.Warning, Message = "Disconnect detected in connection. Reconnecting...")]
//        public void DisconnectDetectedInConnection()
//        {
//            WriteEvent(10);
//        }

//        [Event(11, Level = EventLevel.Error, Message = "Error while reading from the compression input stream. Type: {0}, Error: {1}")]
//        public void FailedReadingCompressionStream(string compressionType, string exception)
//        {
//            WriteEvent(11, compressionType, exception);
//        }

//        [Event(12, Level = EventLevel.Warning, Message = "Ignoring unexpected errors on closing ZooKeeperClient: {0}")]
//        public void ErrorsFoundClosingZkClient(bool calledDisconnect, string exception)
//        {
//            WriteEvent(12, exception);
//        }

//        [Event(13, Level = EventLevel.Error, Message = "Unexpected error found in Message send: {0}")]
//        public void UnexpectedErrorInMessageSend(string exception)
//        {
//            WriteEvent(13, exception);
//        }

//        [Event(14, Level = EventLevel.Warning, Message = "Failed to fetch offsets before. Error: {0}, Try Count: {1}")]
//        public void FailedToFetchOffsetsBefore(string exception, short tryCounter)
//        {
//            WriteEvent(14, exception, tryCounter);
//        }

//        [Event(15, Level = EventLevel.Warning, Message = "Error when sending messages to broker: {0}")]
//        public void ErrorFoundSendingMessages(string exception)
//        {
//            WriteEvent(15, exception);
//        }

//        [Event(16, Level = EventLevel.Warning, Message = "Error found closing producer handler: {0}. Ignoring.")]
//        public void ErrorFoundClosingProducerHandler(string exception)
//        {
//            WriteEvent(16, exception);
//        }

//        [Event(17, Level = EventLevel.Warning, Message = "Error connecting to address: {0}, host: {1}, error: {2}")]
//        public void ErrorConnectingToAddress(string address, string host, string exception)
//        {
//            WriteEvent(17, address, host, exception);
//        }

//        [Event(18, Level = EventLevel.Error, Message = "Error received from broker fetching topic metadata. Topic: {0}, Error: {1}")]
//        public void ErrorFoundGettingTopicMetadata(string topic, string error)
//        {
//            WriteEvent(18, topic, error);
//        }
//    }
//}
