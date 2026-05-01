using System.Text.Json;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.DependencyInjection;

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

        using var host = FakeKafkaPipelineHostBuilder
            .Create(testDirectory.Path, fakeKafka)
            .WithTopic(
                index: 0,
                name: topicName,
                keyType: "String",
                compacting: "On",
                exportFileName: exportFile,
                exportRawMessage: true,
                filterKeyType: "Equals",
                filterKeyValue: "customer-1")
            .Build();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Act
        var loaderTool = host.Services.GetRequiredService<ILoaderTool>();
        await loaderTool.ProcessAsync(cts.Token);

        // Assert
        using var output = await ReadJsonArrayAsync(testDirectory.Path, exportFile, cts.Token);
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

        using var host = FakeKafkaPipelineHostBuilder
            .Create(testDirectory.Path, fakeKafka)
            .WithConcurrentLoad()
            .WithTopic(0, "customers", "String", "On", "customers.json", exportRawMessage: true)
            .WithTopic(1, "accounts", "Long", "On", "accounts.json", exportRawMessage: true)
            .Build();
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

        using var host = FakeKafkaPipelineHostBuilder
            .Create(testDirectory.Path, fakeKafka)
            .WithTimeSortAscending()
            .WithTopic(0, topicName, "String", "Off", exportFile, exportRawMessage: false)
            .Build();
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

        using var host = FakeKafkaPipelineHostBuilder
            .Create(testDirectory.Path, fakeKafka)
            .WithTopic(
                index: 0,
                name: topicName,
                keyType: "String",
                compacting: "Off",
                exportFileName: exportFile,
                exportRawMessage: true,
                offsetStartDate: "1970-01-01T00:01:30Z",
                offsetEndDate: "1970-01-01T00:03:00Z",
                partitionIds: [0])
            .Build();
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

        using var host = FakeKafkaPipelineHostBuilder
            .Create(testDirectory.Path, fakeKafka)
            .WithTopic(0, topicName, "Ignored", "Off", exportFile, exportRawMessage: true)
            .Build();
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
}
