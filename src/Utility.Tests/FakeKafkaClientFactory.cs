using System.Text;

using Confluent.Kafka;

using KafkaSnapshot.Import.Kafka;

using Moq;

namespace KafkaSnapshot.Utility.Tests;

internal sealed class FakeKafkaClientFactory : IKafkaClientFactory
{
    public FakeKafkaClientFactory WithTopic<TKey>(
        string topicName,
        params FakeKafkaMessage<TKey>[] messages)
        where TKey : notnull
    {
        _topics[topicName] = [.. messages.Select(message => message.ToStoredMessage())];
        return this;
    }

    public IAdminClient CreateAdminClient()
    {
        var adminClient = new Mock<IAdminClient>(MockBehavior.Strict);

        foreach (var topic in _topics)
        {
            var metadata = CreateMetadata(topic.Key, topic.Value);
            adminClient
                .Setup(client => client.GetMetadata(topic.Key, It.IsAny<TimeSpan>()))
                .Returns(metadata);
        }

        adminClient.Setup(client => client.Dispose());

        return adminClient.Object;
    }

    public IConsumer<TKey, byte[]> CreateConsumer<TKey>()
    {
        var consumer = new Mock<IConsumer<TKey, byte[]>>(MockBehavior.Strict);
        TopicPartition? assignedPartition = null;

        consumer
            .Setup(client => client.QueryWatermarkOffsets(
                It.IsAny<TopicPartition>(),
                It.IsAny<TimeSpan>()))
            .Returns<TopicPartition, TimeSpan>((topicPartition, _) =>
            {
                var messages = GetPartitionMessages(topicPartition);
                return new WatermarkOffsets(new Offset(0), new Offset(messages.Count));
            });

        consumer
            .Setup(client => client.Assign(It.IsAny<TopicPartition>()))
            .Callback<TopicPartition>(topicPartition =>
            {
                assignedPartition = topicPartition;
                _consumeIndexes[topicPartition] = 0;
            });

        consumer
            .Setup(client => client.Assign(It.IsAny<TopicPartitionOffset>()))
            .Callback<TopicPartitionOffset>(topicPartitionOffset =>
            {
                assignedPartition = topicPartitionOffset.TopicPartition;
                var messages = GetPartitionMessages(assignedPartition);
                var index = messages.FindIndex(message => message.Offset >= topicPartitionOffset.Offset.Value);
                _consumeIndexes[assignedPartition] = index < 0 ? messages.Count : index;
            });

        consumer
            .Setup(client => client.OffsetsForTimes(
                It.IsAny<IEnumerable<TopicPartitionTimestamp>>(),
                It.IsAny<TimeSpan>()))
            .Returns<IEnumerable<TopicPartitionTimestamp>, TimeSpan>((timestamps, _) =>
                [.. timestamps.Select(timestamp =>
                {
                    var messages = GetPartitionMessages(timestamp.TopicPartition);
                    var message = messages.FirstOrDefault(
                        item => item.Timestamp >= timestamp.Timestamp.UtcDateTime);
                    var offset = message is null
                        ? Offset.End
                        : new Offset(message.Offset);

                    return new TopicPartitionOffset(timestamp.TopicPartition, offset);
                })]);

        consumer
            .Setup(client => client.Consume(It.IsAny<CancellationToken>()))
            .Returns<CancellationToken>(_ =>
            {
                if (assignedPartition is null)
                {
                    throw new InvalidOperationException("Consumer was not assigned to a topic partition.");
                }

                var messages = GetPartitionMessages(assignedPartition);
                var index = _consumeIndexes.GetValueOrDefault(assignedPartition);
                _consumeIndexes[assignedPartition] = index + 1;

                if (index >= messages.Count)
                {
                    throw new InvalidOperationException(
                        $"No fake Kafka messages left for {assignedPartition}.");
                }

                var message = messages[index];

                return new ConsumeResult<TKey, byte[]>
                {
                    TopicPartitionOffset = new TopicPartitionOffset(
                        assignedPartition,
                        new Offset(message.Offset)),
                    Message = new Message<TKey, byte[]>
                    {
                        Key = (TKey)message.Key,
                        Value = Encoding.UTF8.GetBytes(message.Value),
                        Timestamp = new Timestamp(message.Timestamp)
                    }
                };
            });

        consumer.Setup(client => client.Close());
        consumer.Setup(client => client.Dispose());

        return consumer.Object;
    }

    private List<StoredKafkaMessage> GetPartitionMessages(TopicPartition topicPartition)
        => [.. _topics[topicPartition.Topic]
            .Where(message => message.Partition == topicPartition.Partition.Value)
            .OrderBy(message => message.Offset)];

    private static Metadata CreateMetadata(
        string topicName,
        IEnumerable<StoredKafkaMessage> messages)
    {
        var partitionMetadata = messages
            .Select(message => message.Partition)
            .Distinct()
            .Select(partition => new PartitionMetadata(partition, 1, [1], [1], null))
            .ToList();

        var brokerMetadata = new BrokerMetadata(1, "fake-kafka", 9092);
        var topicMetadata = new TopicMetadata(topicName, partitionMetadata, null);

        return new Metadata([brokerMetadata], [topicMetadata], 1, "fake-kafka");
    }

    private readonly Dictionary<string, IReadOnlyList<StoredKafkaMessage>> _topics = [];
    private readonly Dictionary<TopicPartition, int> _consumeIndexes = [];
}

