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

    [Fact(DisplayName = "Utility host can process multiple topics concurrently with fake Kafka layer.")]
    [Trait("Category", "Integration")]
    public async Task UtilityHostCanProcessMultipleTopicsConcurrentlyWithFakeKafkaLayer()
    {
        // Arrange
        using var testDirectory = new TestDirectory();
        var fakeKafka = new FakeKafkaClientFactory()
            .WithTopic(
                "customers",
                new FakeKafkaMessage<string>("customer-1", "active", 0, DateTime.UnixEpoch.AddMinutes(1)),
                new FakeKafkaMessage<string>("customer-1", "active-updated", 1, DateTime.UnixEpoch.AddMinutes(2)))
            .WithTopic(
                "accounts",
                new FakeKafkaMessage<long>(101L, "created", 0, DateTime.UnixEpoch.AddMinutes(1)),
                new FakeKafkaMessage<long>(101L, "closed", 1, DateTime.UnixEpoch.AddMinutes(2)));

        using var host = CreateHost(
            testDirectory.Path,
            fakeKafka,
            new Dictionary<string, string?>
            {
                ["LoaderToolConfiguration:UseConcurrentLoad"] = "true",
                ["LoaderToolConfiguration:Topics:0:Name"] = "customers",
                ["LoaderToolConfiguration:Topics:0:KeyType"] = "String",
                ["LoaderToolConfiguration:Topics:0:Compacting"] = "On",
                ["LoaderToolConfiguration:Topics:0:ExportFileName"] = "customers.json",
                ["LoaderToolConfiguration:Topics:0:ExportRawMessage"] = "true",
                ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = "None",
                ["LoaderToolConfiguration:Topics:1:Name"] = "accounts",
                ["LoaderToolConfiguration:Topics:1:KeyType"] = "Long",
                ["LoaderToolConfiguration:Topics:1:Compacting"] = "On",
                ["LoaderToolConfiguration:Topics:1:ExportFileName"] = "accounts.json",
                ["LoaderToolConfiguration:Topics:1:ExportRawMessage"] = "true",
                ["LoaderToolConfiguration:Topics:1:FilterKeyType"] = "None"
            });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Act
        var loaderTool = host.Services.GetRequiredService<ILoaderTool>();
        await loaderTool.ProcessAsync(cts.Token);

        // Assert
        using var customers = await ReadJsonArrayAsync(testDirectory.Path, "customers.json", cts.Token);
        customers.RootElement.GetArrayLength().Should().Be(1);
        customers.RootElement[0].GetProperty("Key").GetString().Should().Be("customer-1");
        customers.RootElement[0].GetProperty("Value").GetString().Should().Be("active-updated");

        using var accounts = await ReadJsonArrayAsync(testDirectory.Path, "accounts.json", cts.Token);
        accounts.RootElement.GetArrayLength().Should().Be(1);
        accounts.RootElement[0].GetProperty("Key").GetInt64().Should().Be(101L);
        accounts.RootElement[0].GetProperty("Value").GetString().Should().Be("closed");
    }

    [Fact(DisplayName = "Utility host can export non compacted JSON messages sorted by timestamp.")]
    [Trait("Category", "Integration")]
    public async Task UtilityHostCanExportNonCompactedJsonMessagesSortedByTimestamp()
    {
        // Arrange
        using var testDirectory = new TestDirectory();
        var topicName = "orders";
        var exportFile = "orders.json";
        var fakeKafka = new FakeKafkaClientFactory()
            .WithTopic(
                topicName,
                new FakeKafkaMessage<string>(
                    "order-2",
                    CreateOrderMessage(status: "created", version: 1),
                    0,
                    DateTime.UnixEpoch.AddMinutes(3)),
                new FakeKafkaMessage<string>(
                    "order-1",
                    CreateOrderMessage(status: "created", version: 1),
                    1,
                    DateTime.UnixEpoch.AddMinutes(1)),
                new FakeKafkaMessage<string>(
                    "order-1",
                    CreateOrderMessage(status: "paid", version: 2),
                    2,
                    DateTime.UnixEpoch.AddMinutes(2)));

        using var host = CreateHost(
            testDirectory.Path,
            fakeKafka,
            new Dictionary<string, string?>
            {
                ["LoaderToolConfiguration:Topics:0:Name"] = topicName,
                ["LoaderToolConfiguration:Topics:0:KeyType"] = "String",
                ["LoaderToolConfiguration:Topics:0:Compacting"] = "Off",
                ["LoaderToolConfiguration:Topics:0:ExportFileName"] = exportFile,
                ["LoaderToolConfiguration:Topics:0:ExportRawMessage"] = "false",
                ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = "None",
                ["LoaderToolConfiguration:GlobalSortOrder"] = "Ascending",
                ["LoaderToolConfiguration:GlobalMessageSort"] = "Time"
            });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Act
        var loaderTool = host.Services.GetRequiredService<ILoaderTool>();
        await loaderTool.ProcessAsync(cts.Token);

        // Assert
        using var output = await ReadJsonArrayAsync(testDirectory.Path, exportFile, cts.Token);
        var root = output.RootElement;

        root.GetArrayLength().Should().Be(3);
        root[0].GetProperty("Key").GetString().Should().Be("order-1");
        root[0].GetProperty("Value").GetProperty("status").GetString().Should().Be("created");
        root[1].GetProperty("Key").GetString().Should().Be("order-1");
        root[1].GetProperty("Value").GetProperty("status").GetString().Should().Be("paid");
        root[2].GetProperty("Key").GetString().Should().Be("order-2");
        root[2].GetProperty("Value").GetProperty("status").GetString().Should().Be("created");
    }

    [Fact(DisplayName = "Utility host can apply date range and partition filter with fake Kafka layer.")]
    [Trait("Category", "Integration")]
    public async Task UtilityHostCanApplyDateRangeAndPartitionFilterWithFakeKafkaLayer()
    {
        // Arrange
        using var testDirectory = new TestDirectory();
        var topicName = "events";
        var exportFile = "events.json";
        var fakeKafka = new FakeKafkaClientFactory()
            .WithTopic(
                topicName,
                new FakeKafkaMessage<string>("event-0", "too-old", 0, DateTime.UnixEpoch.AddMinutes(1), Partition: 0),
                new FakeKafkaMessage<string>("event-1", "inside-range", 1, DateTime.UnixEpoch.AddMinutes(2), Partition: 0),
                new FakeKafkaMessage<string>("event-2", "too-new", 2, DateTime.UnixEpoch.AddMinutes(4), Partition: 0),
                new FakeKafkaMessage<string>("event-3", "wrong-partition", 0, DateTime.UnixEpoch.AddMinutes(2), Partition: 1));

        using var host = CreateHost(
            testDirectory.Path,
            fakeKafka,
            new Dictionary<string, string?>
            {
                ["LoaderToolConfiguration:Topics:0:Name"] = topicName,
                ["LoaderToolConfiguration:Topics:0:KeyType"] = "String",
                ["LoaderToolConfiguration:Topics:0:Compacting"] = "Off",
                ["LoaderToolConfiguration:Topics:0:ExportFileName"] = exportFile,
                ["LoaderToolConfiguration:Topics:0:ExportRawMessage"] = "true",
                ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = "None",
                ["LoaderToolConfiguration:Topics:0:OffsetStartDate"] = "1970-01-01T00:01:30Z",
                ["LoaderToolConfiguration:Topics:0:OffsetEndDate"] = "1970-01-01T00:03:00Z",
                ["LoaderToolConfiguration:Topics:0:PartitionsIds:0"] = "0"
            });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Act
        var loaderTool = host.Services.GetRequiredService<ILoaderTool>();
        await loaderTool.ProcessAsync(cts.Token);

        // Assert
        using var output = await ReadJsonArrayAsync(testDirectory.Path, exportFile, cts.Token);
        var root = output.RootElement;

        root.GetArrayLength().Should().Be(1);
        root[0].GetProperty("Key").GetString().Should().Be("event-1");
        root[0].GetProperty("Value").GetString().Should().Be("inside-range");
        root[0].GetProperty("Meta").GetProperty("Partition").GetInt32().Should().Be(0);
        root[0].GetProperty("Meta").GetProperty("Offset").GetInt64().Should().Be(1);
    }

    [Fact(DisplayName = "Utility host can export ignored keys with fake Kafka layer.")]
    [Trait("Category", "Integration")]
    public async Task UtilityHostCanExportIgnoredKeysWithFakeKafkaLayer()
    {
        // Arrange
        using var testDirectory = new TestDirectory();
        var topicName = "audit";
        var exportFile = "audit.json";
        var fakeKafka = new FakeKafkaClientFactory()
            .WithTopic(
                topicName,
                new FakeKafkaMessage<string>("ignored-1", "created", 0, DateTime.UnixEpoch.AddMinutes(1)),
                new FakeKafkaMessage<string>("ignored-2", "updated", 1, DateTime.UnixEpoch.AddMinutes(2)));

        using var host = CreateHost(
            testDirectory.Path,
            fakeKafka,
            new Dictionary<string, string?>
            {
                ["LoaderToolConfiguration:Topics:0:Name"] = topicName,
                ["LoaderToolConfiguration:Topics:0:KeyType"] = "Ignored",
                ["LoaderToolConfiguration:Topics:0:Compacting"] = "Off",
                ["LoaderToolConfiguration:Topics:0:ExportFileName"] = exportFile,
                ["LoaderToolConfiguration:Topics:0:ExportRawMessage"] = "true",
                ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = "None"
            });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Act
        var loaderTool = host.Services.GetRequiredService<ILoaderTool>();
        await loaderTool.ProcessAsync(cts.Token);

        // Assert
        using var output = await ReadJsonArrayAsync(testDirectory.Path, exportFile, cts.Token);
        var root = output.RootElement;

        root.GetArrayLength().Should().Be(2);
        root[0].TryGetProperty("Key", out _).Should().BeFalse();
        root[0].GetProperty("Value").GetString().Should().Be("created");
        root[1].TryGetProperty("Key", out _).Should().BeFalse();
        root[1].GetProperty("Value").GetString().Should().Be("updated");
    }

    private static IHost CreateHost(
        string outputDirectory,
        string topicName,
        string exportFile,
        IKafkaClientFactory kafkaClientFactory)
        => CreateHost(
            outputDirectory,
            kafkaClientFactory,
            new Dictionary<string, string?>
            {
                ["LoaderToolConfiguration:Topics:0:Name"] = topicName,
                ["LoaderToolConfiguration:UseConcurrentLoad"] = "false",
                ["LoaderToolConfiguration:Topics:0:KeyType"] = "String",
                ["LoaderToolConfiguration:Topics:0:Compacting"] = "On",
                ["LoaderToolConfiguration:Topics:0:ExportFileName"] = exportFile,
                ["LoaderToolConfiguration:Topics:0:ExportRawMessage"] = "true",
                ["LoaderToolConfiguration:Topics:0:FilterKeyType"] = "Equals",
                ["LoaderToolConfiguration:Topics:0:FilterKeyValue"] = "customer-1"
            });

    private static IHost CreateHost(
        string outputDirectory,
        IKafkaClientFactory kafkaClientFactory,
        IReadOnlyDictionary<string, string?> scenarioConfiguration)
    {
        var configuration = new Dictionary<string, string?>
        {
            ["LoaderToolConfiguration:UseConcurrentLoad"] = "false",
            ["JsonFileDataExporterConfiguration:OutputDirectory"] = outputDirectory,
            ["BootstrapServersConfiguration:BootstrapServers:0"] = "fake-kafka:9092",
            ["TopicWatermarkLoaderConfiguration:AdminClientTimeout"] = "00:00:01",
            ["SnapshotLoaderConfiguration:DateOffsetTimeout"] = "00:00:01",
            ["SnapshotLoaderConfiguration:MaxConcurrentPartitions"] = "1"
        };

        foreach (var (key, value) in scenarioConfiguration)
        {
            configuration[key] = value;
        }

        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(configuration);

        _ = builder.Services.AddKafkaSnapshotUtility(builder.Configuration);
        _ = builder.Services.AddSingleton(kafkaClientFactory);

        return builder.Build();
    }

    private static async Task<JsonDocument> ReadJsonArrayAsync(
        string outputDirectory,
        string fileName,
        CancellationToken ct)
    {
        var outputFile = Path.Combine(outputDirectory, fileName);
        File.Exists(outputFile).Should().BeTrue();

        return JsonDocument.Parse(await File.ReadAllTextAsync(outputFile, ct));
    }

    private static string CreateOrderMessage(string status, int version)
        => JsonSerializer.Serialize(new { status, version });

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
