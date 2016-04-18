# CSharpClient-for-Kafka

[![Join the chat at https://gitter.im/Microsoft/Kafkanet](https://badges.gitter.im/Microsoft/Kafkanet.svg)](https://gitter.im/Microsoft/Kafkanet?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
.Net implementation of the Apache Kafka Protocol that provides basic functionality through Producer/Consumer classes. The project also offers balanced consumer implementation. 
The project is a fork from ExactTarget's Kafka-net Client.

## Related documentation
* [Kafka documentation](https://kafka.apache.org/documentation.html)
* [Zookeeper documentation](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Index)
* [Kafka client protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

## Build CSharpClient-for-Kafka
* Clone CSharpClient-for-Kafka through ```git clone https://github.com/Microsoft/CSharpClient-for-Kafka.git```
* Open `src\KafkaNETLibraryAndConsole.sln` in Visual Studio
* Build Solution

## Run Unit Tests
* Open Test Window in Visual Studio: Test>Windows>Test Explorer
* Run all

## Using Console
* Setup local Kafka and Zookeeper

### Console Options
		topic                           Dump topics metadata, such as: earliest/latest offset, replica, ISR.
        consumesimple                   Consume data in single thread.
        consumegroup                    Monitor latest offset gap and speed of consumer group.
        consumegroupmonitor             Monitor latest offset gap and speed of consumer group.
        producesimple                   Produce data in single thread.
        produceperftest                 Produce data in multiple thread.
        eventserverperftest             Http Post data to event server in multiple thread.
        producemonitor                  Monitor latest offset.
        test                            Run some adhoc test cases.
		
## Using the library

### Producer

The Producer can send one message or an entire batch to Kafka. When sending a batch you can send to multiple topics at once
#### Producer Usage

```c#
var brokerConfig = new BrokerConfiguration()
{
    BrokerId = this.brokerId,
    Host = this.kafkaServerName,
    Port = this.kafkaPort
};
var config = new ProducerConfiguration(new List<BrokerConfiguration> { brokerConfig });
kafkaProducer = new Producer(config);
// here you construct your batch or a single message object
var batch=ConstructBatch();
kafkaProducer.Send(batch);
```

### Simple Consumer

The simple Consumer allows full control for retrieving data. You could instantiate a Consumer directly by providing a ConsumerConfiguration and then calling Fetch.
CSharpClient-for-Kafka has a higher level wrapper around Consumer which allows consumer reuse and other benefits
#### Consumer Usage

```c#
// create the Consumer higher level manager
var managerConfig = new KafkaSimpleManagerConfiguration()
{
    FetchSize = FetchSize,
    BufferSize = BufferSize,
    Zookeeper = m_zookeeper
};
m_consumerManager = new KafkaSimpleManager<int, Kafka.Client.Messages.Message>(managerConfig);
// get all available partitions for a topic through the manager
var allPartitions = m_consumerManager.GetTopicPartitionsFromZK(m_topic);
// Refresh metadata and grab a consumer for desired partitions
m_consumerManager.RefreshMetadata(0, m_consumerId, 0, m_topic, true);
var partitionConsumer = m_consumerManager.GetConsumer(m_topic, partitionId);
```
### Balanced Consumer

The balanced consumer manages partition assignment for each instance in the same consumer group. Rebalance are triggered by zookeeper changes.
#### Balanced Consumer Usage

```c#
// Here we create a balanced consumer on one consumer machine for consumerGroupId. All machines consuming for this group will get balanced together
ConsumerConfiguration config = new ConsumerConfiguration
{
    AutoCommit = false,
    GroupId = consumerGroupId
    ConsumerId = uniqueConsumerId
    MaxFetchBufferLength = m_BufferMaxNoOfMessages,
    FetchSize = fetchSize,
    AutoOffsetReset = OffsetRequest.LargestTime,
    NumberOfTries = 20,
    ZooKeeper = new ZooKeeperConfiguration(zookeeperString, 30000, 30000, 2000)
};
var balancedConsumer = new ZookeeperConsumerConnector(config, true, m_ConsumerRebalanceHandler, m_ZKDisconnectHandler, m_ZKExpireHandler);
// grab streams for desired topics 
var streams = m_ZooKeeperConsumerConnector.CreateMessageStreams(m_TopicMap, new DefaultDecoder());
var KafkaMessageStream = streams[m_Topic][0];
// start consuming stream
foreach (Message message in m_KafkaMessageStream.GetCancellable(cancellationTokenSource.Token))
....
```

## Contribute

Contributions to CSharpClient-for-Kafka are welcome.  Here is how you can contribute to CSharpClient-for-Kafka:
* [Submit bugs](https://github.com/Microsoft/CSharpClient-for-Kafka/issues) and help us verify fixes
* [Submit pull requests](https://github.com/Microsoft/CSharpClient-for-Kafka/pulls) for bug fixes and features and discuss existing proposals
