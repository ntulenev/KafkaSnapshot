using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Unit")]
public class PartitionSnapshotStreamingTests
{
    [Fact(DisplayName = "Streaming applies bounded backpressure and disposes the producer on early exit.")]
    public async Task BoundedInputStopsOnEarlyDisposal()
    {
        var produced = 0;
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        IEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source(CancellationToken ct)
        {
            try
            {
                for (var i = 0; i < 1000; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    if (Interlocked.Increment(ref produced) == 3)
                    {
                        blocked.SetResult();
                    }

                    yield return Message(i);
                }
            }
            finally
            {
                closed.SetResult();
            }
        }

        var (reader, topic, watermarks, key, value) = Setup((_, ct) => Source(ct));
        var enumerator = reader.StreamAsync(watermarks, topic, key, value, CancellationToken.None).GetAsyncEnumerator();
        try
        {
            (await enumerator.MoveNextAsync()).Should().BeTrue();
            await blocked.Task.WaitAsync(TestTimeouts.Wait);
            Volatile.Read(ref produced).Should().Be(3);
        }
        finally
        {
            await enumerator.DisposeAsync().AsTask().WaitAsync(TestTimeouts.Wait);
        }

        await closed.Task.WaitAsync(TestTimeouts.Wait);
    }

    [Fact(DisplayName = "Canceling a streaming reader interrupts a blocking partition read and joins its worker.")]
    public async Task CancellationStopsBlockedReader()
    {
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = false;
        IEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source(CancellationToken ct)
        {
            try
            {
                yield return Message(0);
                blocked.SetResult();
                ct.WaitHandle.WaitOne();
                ct.ThrowIfCancellationRequested();
            }
            finally
            {
                closed = true;
            }
        }

        var (reader, topic, watermarks, key, value) = Setup((_, ct) => Source(ct));
        using var cts = new CancellationTokenSource();
        await using var enumerator = reader.StreamAsync(watermarks, topic, key, value, cts.Token).GetAsyncEnumerator();
        (await enumerator.MoveNextAsync()).Should().BeTrue();
        await blocked.Task.WaitAsync(TestTimeouts.Wait);
        await cts.CancelAsync();
        Func<Task> act = async () => await enumerator.MoveNextAsync();
        await act.Should().ThrowAsync<OperationCanceledException>();
        closed.Should().BeTrue();
    }

    [Fact(DisplayName = "Streaming propagates the original producer failure.")]
    public async Task ProducerFailureIsPropagated()
    {
        var failure = new InvalidOperationException("reader failed");
        IEnumerable<KeyValuePair<string, KafkaMessage<string>>> Source()
        {
            yield return Message(0);
            throw failure;
        }

        var (reader, topic, watermarks, key, value) = Setup((_, _) => Source());
        Func<Task> act = async () => await Collect(reader.StreamAsync(watermarks, topic, key, value, CancellationToken.None));
        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
    }

    [Theory(DisplayName = "Streaming preserves partition order and stops at the first matching partition when configured.")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TraversalAndFirstMatchArePreserved(bool firstOnly)
    {
        var visited = new List<int>();
        var (reader, topic, watermarks, key, value) = Setup((watermark, _) =>
        {
            visited.Add(watermark.Partition);
            return watermark.Partition == 0 ? [] : [Message(watermark.Partition)];
        }, firstOnly);
        var result = await Collect(reader.StreamAsync(watermarks, topic, key, value, CancellationToken.None));
        result.Select(x => x.Value.Meta.Offset).Should().Equal(firstOnly ? [1L] : [1L, 2L]);
        visited.Should().Equal(firstOnly ? [0, 1] : [0, 1, 2]);
    }

    [Fact(DisplayName = "Streaming supports an empty partition set and honors prior cancellation.")]
    public async Task EmptyAndCanceledStreams()
    {
        var (reader, topic, _, key, value) = Setup((_, _) => throw new InvalidOperationException("Unexpected read"));
        (await Collect(reader.StreamAsync([], topic, key, value, CancellationToken.None))).Should().BeEmpty();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        Func<Task> act = async () => await Collect(reader.StreamAsync([], topic, key, value, cts.Token));
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    private static (PartitionSnapshotBatchReader<string, string> Reader, LoadingTopic Topic,
        PartitionWatermark[] Watermarks, IDataFilter<string> Key, IDataFilter<string> Value) Setup(
        Func<PartitionWatermark, CancellationToken, IEnumerable<KeyValuePair<string, KafkaMessage<string>>>> source,
        bool firstOnly = false)
    {
        var topic = new LoadingTopic(new TopicName("orders"), false, new DateFilterRange(null, null), EncoderRules.String, null);
        var partitionReader = new Mock<IPartitionSnapshotReader<string, string>>(MockBehavior.Strict);
        partitionReader.Setup(x => x.Read(It.IsAny<PartitionWatermark>(), topic,
            It.IsAny<IDataFilter<string>>(), It.IsAny<IDataFilter<string>>(), It.IsAny<CancellationToken>()))
            .Returns<PartitionWatermark, LoadingTopic, IDataFilter<string>, IDataFilter<string>, CancellationToken>(
                (watermark, _, _, _, ct) => source(watermark, ct));
        var reader = new PartitionSnapshotBatchReader<string, string>(Options.Create(
            new SnapshotLoaderConfiguration { StreamBufferCapacity = 1, SearchSinglePartition = firstOnly }), partitionReader.Object);
        return (reader, topic, [.. Enumerable.Range(0, 3).Select(i => new PartitionWatermark(topic, new OffsetRange(0, 10), i))],
            Mock.Of<IDataFilter<string>>(), Mock.Of<IDataFilter<string>>());
    }

    private static KeyValuePair<string, KafkaMessage<string>> Message(int offset)
        => new("key", new("value", new(DateTimeOffset.UnixEpoch, 0, offset)));

    private static async Task<List<T>> Collect<T>(IAsyncEnumerable<T> source)
    {
        var result = new List<T>();
        await foreach (var item in source)
        {
            result.Add(item);
        }

        return result;
    }
}
