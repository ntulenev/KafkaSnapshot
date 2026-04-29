using System.Text;
using System.Text.Json;

using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Utility.DependencyInjection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Moq;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

public class FullPipelineWithFakeKafkaIntegrationTests
{
    [Fact(DisplayName = "Utility host can process topic with fake Kafka layer.")]
    [Trait("Category", "Integration")]
    public async Task UtilityHostCanProcessTopicWithFakeKafkaLayer()
    {
        // Arrange
        using var testDirectory = new TestDirectory();
        var topicName = "customers";
        var exportFile = "customers.json";
        var fakeKafka = new FakeKafkaClientFactory()
            .WithTopic(
                topicName,
                new FakeKafkaMessage<string>("customer-1", "active", 0, DateTime.UnixEpoch.AddMinutes(1)),
                new FakeKafkaMessage<string>("customer-2", "inactive", 1, DateTime.UnixEpoch.AddMinutes(2)),
                new FakeKafkaMessage<string>("customer-1", "active-updated", 2, DateTime.UnixEpoch.AddMinutes(3)));

        using var host = CreateHost(testDirectory.Path, topicName, exportFile, fakeKafka);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Act
        var loaderTool = host.Services.GetRequiredService<ILoaderTool>();
        await loaderTool.ProcessAsync(cts.Token);

        // Assert
        var outputFile = Path.Combine(testDirectory.Path, exportFile);
        File.Exists(outputFile).Should().BeTrue();

        using var output = JsonDocument.Parse(await File.ReadAllTextAsync(outputFile, cts.Token));
        var root = output.RootElement;

        root.GetArrayLength().Should().Be(1);
        var item = root[0];
        item.GetProperty("Key").GetString().Should().Be("customer-1");
        item.GetProperty("Value").GetString().Should().Be("active-updated");
        item.GetProperty("Meta").GetProperty("Partition").GetInt32().Should().Be(0);
        item.GetProperty("Meta").GetProperty("Offset").GetInt64().Should().Be(2);
    }

    private static IHost CreateHost(
        string outputDirectory,
        string topicName,
        string exportFile,
        IKafkaClientFactory kafkaClientFactory)
    {
        var configuration = new Dictionary<string, string?>
        {
            ["LoaderToolConfiguration:Topics:0:Name"] = topicName,
            ["LoaderToolConfiguration:UseConcurrentLoad"] = "false",
            ["LoaderToolConfiguration:Topics:0:KeyType"] = "String",
            ["LoaderToolConfiguration:Topics:0:Compacting"] = "On",
            ["LoaderToolConfiguration:Topics:0:ExportFileName"] = exportFile,
            ["LoaderToolConfiguration:Topics:0:ExportRawMessage"] = "true",
            ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = "Equals",
            ["LoaderToolConfiguration:Topics:0:FilterKeyValue"] = "customer-1",
            ["JsonFileDataExporterConfiguration:OutputDirectory"] = outputDirectory,
            ["BootstrapServersConfiguration:BootstrapServers:0"] = "fake-kafka:9092",
            ["TopicWatermarkLoaderConfiguration:AdminClientTimeout"] = "00:00:01",
            ["SnapshotLoaderConfiguration:DateOffsetTimeout"] = "00:00:01",
            ["SnapshotLoaderConfiguration:MaxConcurrentPartitions"] = "1"
        };

        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(configuration);

        _ = builder.Services.AddKafkaSnapshotUtility(builder.Configuration);
        _ = builder.Services.AddSingleton(kafkaClientFactory);

        return builder.Build();
    }

    private sealed class TestDirectory : IDisposable
    {
        public TestDirectory()
        {
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                $"KafkaSnapshot-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(Path);
        }

        public string Path { get; }

        public void Dispose()
        {
            if (Directory.Exists(Path))
            {
                Directory.Delete(Path, recursive: true);
            }
        }
    }

    private sealed class FakeKafkaClientFactory : IKafkaClientFactory
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
                .Callback<TopicPartition>(topicPartition => assignedPartition = topicPartition);

            consumer
                .Setup(client => client.Assign(It.IsAny<TopicPartitionOffset>()))
                .Callback<TopicPartitionOffset>(topicPartitionOffset =>
                    assignedPartition = topicPartitionOffset.TopicPartition);

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

    private sealed record FakeKafkaMessage<TKey>(
        TKey Key,
        string Value,
        long Offset,
        DateTime Timestamp,
        int Partition = 0)
        where TKey : notnull
    {
        public StoredKafkaMessage ToStoredMessage()
            => new(Key, Value, Offset, Timestamp, Partition);
    }

    private sealed record StoredKafkaMessage(
        object Key,
        string Value,
        long Offset,
        DateTime Timestamp,
        int Partition);
}
