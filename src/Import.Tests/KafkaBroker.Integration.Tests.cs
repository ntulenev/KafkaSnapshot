using System.Runtime.CompilerServices;
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
        await WithTopic(async (topic, cancellationToken) =>
        {
            using var producer = new ProducerBuilder<string, byte[]>(ProducerConfiguration(Guid.NewGuid().ToString("N"))).Build();
            producer.InitTransactions(TimeSpan.FromSeconds(20));
            for (var i = 0; i < 3; i++)
            {
                producer.BeginTransaction();
                await producer.ProduceAsync(topic, Message(i), cancellationToken);
                producer.CommitTransaction(TimeSpan.FromSeconds(20));
            }

            var loading = Loading(topic);
            var watermark = Capture(loading, cancellationToken);
            var result = await Read(watermark, loading, cancellationToken);
            result.Should().HaveCount(3);
            result.Select(x => x.Value.Meta.Offset).Should().Equal(0L, 2L, 4L);
            watermark.Offset.High.Should().Be(6);
        });
    }

    [KafkaFact(DisplayName = "Broker: captured snapshots exclude messages appended after watermark capture.")]
    public async Task GrowingPartitionDoesNotExtendSnapshot()
    {
        await WithTopic(async (topic, cancellationToken) =>
        {
            await Produce(topic, 3, cancellationToken);
            var loading = Loading(topic);
            var watermark = Capture(loading, cancellationToken);
            await Produce(topic, 2, cancellationToken);
            var result = await Read(watermark, loading, cancellationToken);
            result.Select(x => x.Value.Meta.Offset).Should().Equal(0L, 1L, 2L);
        });
    }

    [KafkaFact(DisplayName = "Broker: deleting the captured data before reading still terminates at the captured end.")]
    public async Task RetentionDoesNotHangSnapshot()
    {
        await WithTopic(async (topic, cancellationToken) =>
        {
            await Produce(topic, 2, cancellationToken);
            var loading = Loading(topic);
            var watermark = Capture(loading, cancellationToken);
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
            await admin.DeleteRecordsAsync([new TopicPartitionOffset(topic, 0, watermark.Offset.High)],
                new DeleteRecordsOptions { RequestTimeout = TestTimeouts.Wait }).WaitAsync(cancellationToken);
            (await Read(watermark, loading, cancellationToken)).Should().BeEmpty();
        });
    }

    [KafkaFact(DisplayName = "Broker: empty partitions produce empty snapshots.")]
    public async Task EmptyPartitionCompletes()
    {
        await WithTopic(async (topic, cancellationToken) =>
        {
            var loading = Loading(topic);
            (await Read(Capture(loading, cancellationToken), loading, cancellationToken)).Should().BeEmpty();
        });
    }

    [KafkaFact(DisplayName = "Broker: inclusive end dates retain stop-on-first-later-timestamp behavior.")]
    public async Task NonMonotonicTimestampsPreserveContract()
    {
        await WithTopic(async (topic, cancellationToken) =>
        {
            using var producer = new ProducerBuilder<string, byte[]>(ProducerConfiguration()).Build();
            var start = DateTimeOffset.UtcNow.AddMinutes(-5);
            foreach (var seconds in new[] { 10, 20, 30, 15 })
            {
                await producer.ProduceAsync(topic, new Message<string, byte[]>
                {
                    Key = "key",
                    Value = Encoding.UTF8.GetBytes(seconds.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                    Timestamp = new Timestamp(start.AddSeconds(seconds))
                }, cancellationToken);
            }

            var loading = Loading(topic, new DateFilterRange(null, start.AddSeconds(20)));
            var result = await Read(Capture(loading, cancellationToken), loading, cancellationToken);
            result.Select(x => x.Value.Message).Should().Equal("10", "20");
        });
    }

    [KafkaFact(DisplayName = "Broker: tombstones remain explicit decoding errors.")]
    public async Task NullPayloadIsRejected()
    {
        await WithTopic(async (topic, cancellationToken) =>
        {
            using var producer = new ProducerBuilder<string, byte[]>(ProducerConfiguration()).Build();
            await producer.ProduceAsync(topic, new Message<string, byte[]> { Key = "key", Value = null! }, cancellationToken);
            var loading = Loading(topic);
            Func<Task> act = async () => await Read(Capture(loading, cancellationToken), loading, cancellationToken);
            await act.Should().ThrowAsync<ArgumentNullException>();
        });
    }

    private static string Bootstrap => Environment.GetEnvironmentVariable("KAFKA_SNAPSHOT_TEST_BOOTSTRAP")!;

    private static async Task WithTopic(Func<string, CancellationToken, Task> test)
    {
        var topic = "snapshot-tests-" + Guid.NewGuid().ToString("N");
        using var timeout = TestTimeouts.CreateScenarioCancellation();
        var failure = await Record.ExceptionAsync(async () =>
        {
            await CreateTopic(topic, timeout.Token);
            await test(topic, timeout.Token);
        });

        try
        {
            await DeleteTopic(topic);
        }
        catch (Exception cleanup) when (failure is not null)
        {
            throw new AggregateException("The broker scenario and topic cleanup both failed.", failure, cleanup);
        }

        if (failure is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }

    private static ProducerConfig ProducerConfiguration(string? transactionalId = null)
        => new() { BootstrapServers = Bootstrap, MessageTimeoutMs = 15000, TransactionalId = transactionalId };

    private static async Task CreateTopic(string name, CancellationToken cancellationToken)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = name, NumPartitions = 1, ReplicationFactor = 1 }],
            new CreateTopicsOptions { RequestTimeout = TestTimeouts.Wait }).WaitAsync(cancellationToken);
        using var ready = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        ready.CancelAfter(TestTimeouts.Wait);
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
    }

    private static async Task DeleteTopic(string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        try
        {
            await admin.DeleteTopicsAsync([topic], new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
        }
        catch (DeleteTopicsException exception) when (exception.Results.All(x => x.Error.Code == ErrorCode.UnknownTopicOrPart))
        {
            // Creation can fail before the topic exists; there is nothing to clean up.
        }
    }

    private static LoadingTopic Loading(string topic, DateFilterRange? dates = null)
        => new(new TopicName(topic), false, dates ?? new DateFilterRange(null, null), EncoderRules.String, null);

    private static KafkaClientFactory Factory()
        => new(Options.Create(new BootstrapServersConfiguration { BootstrapServers = [Bootstrap] }));

    private static PartitionWatermark Capture(LoadingTopic loading, CancellationToken cancellationToken)
    {
        using var consumer = Factory().CreateConsumer<string>();
        WatermarkOffsets offsets;
        var timeout = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                offsets = consumer.QueryWatermarkOffsets(new TopicPartition(loading.Value.Name, 0), TimeSpan.FromSeconds(2));
                break;
            }
            catch (KafkaException exception) when (timeout.Elapsed < TimeSpan.FromSeconds(10) &&
                exception.Error.Code is ErrorCode.Local_UnknownPartition or ErrorCode.UnknownTopicOrPart)
            {
                cancellationToken.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(50));
            }
        }
        consumer.Close();
        return KafkaWatermarkMapper.ToDomain(loading, offsets, new Partition(0));
    }

    private static async Task<List<KeyValuePair<string, KafkaSnapshot.Models.Message.KafkaMessage<string>>>> Read(
        PartitionWatermark watermark, LoadingTopic topic, CancellationToken cancellationToken)
    {
        var reader = new PartitionSnapshotReader<string, string>(NullLogger<PartitionSnapshotReader<string, string>>.Instance,
            Options.Create(new SnapshotLoaderConfiguration { DateOffsetTimeout = TimeSpan.FromSeconds(5) }),
            Factory().CreateConsumer<string>, new ByteMessageEncoder(), new PartitionConsumerAssignment());
        var filter = Mock.Of<IDataFilter<string>>(x => x.IsMatch(It.IsAny<string>()));
        return await Task.Run(() => reader.Read(watermark, topic, filter, filter, cancellationToken).ToList(), cancellationToken);
    }

    private static Message<string, byte[]> Message(int value)
        => new() { Key = "key", Value = Encoding.UTF8.GetBytes(value.ToString(System.Globalization.CultureInfo.InvariantCulture)) };

    private static async Task Produce(string topic, int count, CancellationToken cancellationToken)
    {
        using var producer = new ProducerBuilder<string, byte[]>(ProducerConfiguration()).Build();
        for (var i = 0; i < count; i++)
        {
            await producer.ProduceAsync(topic, Message(i), cancellationToken);
        }
    }
}

internal sealed class KafkaFactAttribute : FactAttribute
{
    public KafkaFactAttribute(
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1)
        : base(sourceFilePath, sourceLineNumber)
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("KAFKA_SNAPSHOT_TEST_BOOTSTRAP")))
        {
            Skip = "Run eng/Test-Kafka.ps1 to provision a real Kafka broker.";
        }
    }
}
