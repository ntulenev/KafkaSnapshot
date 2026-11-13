using System.Text;

using Confluent.Kafka;
using Confluent.Kafka.Admin;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Encoders;
using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Broker")]
public class KafkaBrokerIntegrationTests
{
    [KafkaFact(DisplayName = "Broker: control-record gaps and a missing final data offset terminate at EOF.")]
    public async Task TransactionControlOffsetsDoNotHang()
    {
        var topic = await CreateTopic();
        try
        {
            using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = Bootstrap,
                TransactionalId = Guid.NewGuid().ToString("N")
            }).Build();
            producer.InitTransactions(TimeSpan.FromSeconds(20));
            for (var i = 0; i < 3; i++)
            {
                producer.BeginTransaction();
                await producer.ProduceAsync(topic, Message(i));
                producer.CommitTransaction(TimeSpan.FromSeconds(20));
            }

            var loading = Loading(topic);
            var watermark = Capture(loading);
            var result = await Read(watermark, loading);
            result.Should().HaveCount(3);
            result.Select(x => x.Value.Meta.Offset).Should().Equal(0L, 2L, 4L);
            watermark.Offset.High.Should().Be(6);
        }
        finally
        {
            await DeleteTopic(topic);
        }
    }

    [KafkaFact(DisplayName = "Broker: captured snapshots exclude messages appended after watermark capture.")]
    public async Task GrowingPartitionDoesNotExtendSnapshot()
    {
        var topic = await CreateTopic();
        try
        {
            await Produce(topic, 3);
            var loading = Loading(topic);
            var watermark = Capture(loading);
            await Produce(topic, 2);
            var result = await Read(watermark, loading);
            result.Select(x => x.Value.Meta.Offset).Should().Equal(0L, 1L, 2L);
        }
        finally
        {
            await DeleteTopic(topic);
        }
    }

    [KafkaFact(DisplayName = "Broker: deleting the captured data before reading still terminates at the captured end.")]
    public async Task RetentionDoesNotHangSnapshot()
    {
        var topic = await CreateTopic();
        try
        {
            await Produce(topic, 2);
            var loading = Loading(topic);
            var watermark = Capture(loading);
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
            await admin.DeleteRecordsAsync([new TopicPartitionOffset(topic, 0, watermark.Offset.High)]);
            (await Read(watermark, loading)).Should().BeEmpty();
        }
        finally
        {
            await DeleteTopic(topic);
        }
    }

    [KafkaFact(DisplayName = "Broker: empty partitions produce empty snapshots.")]
    public async Task EmptyPartitionCompletes()
    {
        var topic = await CreateTopic();
        try
        {
            var loading = Loading(topic);
            (await Read(Capture(loading), loading)).Should().BeEmpty();
        }
        finally
        {
            await DeleteTopic(topic);
        }
    }

    [KafkaFact(DisplayName = "Broker: inclusive end dates retain stop-on-first-later-timestamp behavior.")]
    public async Task NonMonotonicTimestampsPreserveContract()
    {
        var topic = await CreateTopic();
        try
        {
            using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = Bootstrap, MessageTimeoutMs = 15000 }).Build();
            var start = DateTimeOffset.UtcNow.AddMinutes(-5);
            foreach (var seconds in new[] { 10, 20, 30, 15 })
            {
                await producer.ProduceAsync(topic, new Message<string, byte[]>
                {
                    Key = "key",
                    Value = Encoding.UTF8.GetBytes(seconds.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                    Timestamp = new Timestamp(start.AddSeconds(seconds))
                });
            }

            var loading = Loading(topic, new DateFilterRange(null, start.AddSeconds(20)));
            var result = await Read(Capture(loading), loading);
            result.Select(x => x.Value.Message).Should().Equal("10", "20");
        }
        finally
        {
            await DeleteTopic(topic);
        }
    }

    [KafkaFact(DisplayName = "Broker: tombstones remain explicit decoding errors.")]
    public async Task NullPayloadIsRejected()
    {
        var topic = await CreateTopic();
        try
        {
            using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = Bootstrap, MessageTimeoutMs = 15000 }).Build();
            await producer.ProduceAsync(topic, new Message<string, byte[]> { Key = "key", Value = null! });
            var loading = Loading(topic);
            Func<Task> act = async () => await Read(Capture(loading), loading);
            await act.Should().ThrowAsync<ArgumentNullException>();
        }
        finally
        {
            await DeleteTopic(topic);
        }
    }

    private static string Bootstrap => Environment.GetEnvironmentVariable("KAFKA_SNAPSHOT_TEST_BOOTSTRAP")!;

    private static async Task<string> CreateTopic()
    {
        var name = "snapshot-tests-" + Guid.NewGuid().ToString("N");
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = name, NumPartitions = 1, ReplicationFactor = 1 }],
            new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });
        using var ready = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        while (true)
        {
            ready.Token.ThrowIfCancellationRequested();
            var metadata = admin.GetMetadata(name, TimeSpan.FromSeconds(2));
            if (metadata.Topics.Any(x => x.Topic == name && !x.Error.IsError &&
                x.Partitions.Count == 1 && x.Partitions[0].Leader >= 0))
            {
                break;
            }

            await Task.Delay(100, ready.Token);
        }

        return name;
    }

    private static async Task DeleteTopic(string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        await admin.DeleteTopicsAsync([topic], new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
    }

    private static LoadingTopic Loading(string topic, DateFilterRange? dates = null)
        => new(new TopicName(topic), false, dates ?? new DateFilterRange(null, null), EncoderRules.String, null);

    private static KafkaClientFactory Factory()
        => new(Options.Create(new BootstrapServersConfiguration { BootstrapServers = [Bootstrap] }));

    private static PartitionWatermark Capture(LoadingTopic loading)
    {
        using var consumer = Factory().CreateConsumer<string>();
        WatermarkOffsets offsets;
        var timeout = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            try
            {
                offsets = consumer.QueryWatermarkOffsets(new TopicPartition(loading.Value.Name, 0), TimeSpan.FromSeconds(2));
                break;
            }
            catch (KafkaException exception) when (timeout.Elapsed < TimeSpan.FromSeconds(10) &&
                exception.Error.Code is ErrorCode.Local_UnknownPartition or ErrorCode.UnknownTopicOrPart)
            {
                Thread.Sleep(50);
            }
        }
        consumer.Close();
        return KafkaWatermarkMapper.ToDomain(loading, offsets, new Partition(0));
    }

    private static async Task<List<KeyValuePair<string, KafkaSnapshot.Models.Message.KafkaMessage<string>>>> Read(
        PartitionWatermark watermark, LoadingTopic topic)
    {
        var reader = new PartitionSnapshotReader<string, string>(NullLogger<PartitionSnapshotReader<string, string>>.Instance,
            Options.Create(new SnapshotLoaderConfiguration { DateOffsetTimeout = TimeSpan.FromSeconds(5) }),
            Factory().CreateConsumer<string>, new ByteMessageEncoder(), new PartitionConsumerAssignment());
        var filter = Mock.Of<IDataFilter<string>>(x => x.IsMatch(It.IsAny<string>()) == true);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        return await Task.Run(() => reader.Read(watermark, topic, filter, filter, cts.Token).ToList(), cts.Token)
            .WaitAsync(cts.Token);
    }

    private static Message<string, byte[]> Message(int value)
        => new() { Key = "key", Value = Encoding.UTF8.GetBytes(value.ToString(System.Globalization.CultureInfo.InvariantCulture)) };

    private static async Task Produce(string topic, int count)
    {
        using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = Bootstrap, MessageTimeoutMs = 15000 }).Build();
        for (var i = 0; i < count; i++)
        {
            await producer.ProduceAsync(topic, Message(i));
        }
    }
}

internal sealed class KafkaFactAttribute : FactAttribute
{
    public KafkaFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFKA_SNAPSHOT_TEST_BOOTSTRAP")))
        {
            Skip = "Run eng/Test-Kafka.ps1 to provision a real Kafka broker.";
        }
    }
}
