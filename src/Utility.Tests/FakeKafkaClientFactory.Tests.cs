using Confluent.Kafka;

using FluentAssertions;

using Xunit;

namespace KafkaSnapshot.Utility.Tests;

public class FakeKafkaClientFactoryTests
{
    [Fact(DisplayName = "Fake Kafka consumers keep independent positions for the same partition.")]
    public void ConsumersHaveIndependentPositions()
    {
        var factory = CreateFactory();
        using var first = factory.CreateConsumer<string>();
        using var second = factory.CreateConsumer<string>();
        var partition = new TopicPartition("topic", 0);
        first.Assign(partition);
        second.Assign(new TopicPartitionOffset(partition, 1));

        first.Consume(CancellationToken.None).Offset.Value.Should().Be(0);
        second.Consume(CancellationToken.None).Offset.Value.Should().Be(1);
        second.Assign(partition);
        first.Consume(CancellationToken.None).Offset.Value.Should().Be(1);
        second.Consume(CancellationToken.None).Offset.Value.Should().Be(0);
    }

    [Fact(DisplayName = "Parallel fake Kafka consumers each read the full partition independently.")]
    public async Task ParallelConsumersReadCompletePartitions()
    {
        var factory = CreateFactory();
        using var timeout = new CancellationTokenSource(TestTimeouts.Scenario);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = Enumerable.Range(0, 16).Select(async _ =>
        {
            using var consumer = factory.CreateConsumer<string>();
            consumer.Assign(new TopicPartition("topic", 0));
            await start.Task.WaitAsync(timeout.Token);
            return await Task.Run(() => Enumerable.Range(0, 3)
                .Select(_ => consumer.Consume(timeout.Token).Offset.Value).ToArray(), timeout.Token);
        }).ToArray();

        start.SetResult();
        foreach (var offsets in await Task.WhenAll(reads).WaitAsync(timeout.Token))
        {
            offsets.Should().Equal(0L, 1L, 2L);
        }
    }

    private static FakeKafkaClientFactory CreateFactory()
        => new FakeKafkaClientFactory().WithTopic("topic",
            Enumerable.Range(0, 3).Select(index => new FakeKafkaMessage<string>(
                "key", "value", index, DateTime.UnixEpoch.AddSeconds(index))).ToArray());
}
