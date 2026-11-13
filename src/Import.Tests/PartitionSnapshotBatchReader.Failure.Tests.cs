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
public class PartitionSnapshotBatchReaderFailureTests
{
    [Fact(DisplayName = "A buffered partition failure cancels and joins a sibling blocked in reading.")]
    public async Task FailureStopsBlockedSibling()
    {
        var topic = new LoadingTopic(new TopicName("orders"), false, new DateFilterRange(null, null), EncoderRules.String, null);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = false;
        var failure = new InvalidOperationException("partition failed");
        IEnumerable<KeyValuePair<string, KafkaMessage<string>>> Read(PartitionWatermark watermark, CancellationToken ct)
        {
            if (watermark.Partition == 0)
            {
                try
                {
                    started.SetResult();
                    ct.WaitHandle.WaitOne();
                    ct.ThrowIfCancellationRequested();
                    yield break;
                }
                finally
                {
                    closed = true;
                }
            }

            started.Task.GetAwaiter().GetResult();
            throw failure;
        }

        var partitionReader = new Mock<IPartitionSnapshotReader<string, string>>(MockBehavior.Strict);
        partitionReader.Setup(x => x.Read(It.IsAny<PartitionWatermark>(), topic, It.IsAny<IDataFilter<string>>(),
            It.IsAny<IDataFilter<string>>(), It.IsAny<CancellationToken>()))
            .Returns<PartitionWatermark, LoadingTopic, IDataFilter<string>, IDataFilter<string>, CancellationToken>(
                (watermark, _, _, _, ct) => Read(watermark, ct));
        var reader = new PartitionSnapshotBatchReader<string, string>(Options.Create(
            new SnapshotLoaderConfiguration { MaxConcurrentPartitions = 2 }), partitionReader.Object);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var pending = reader.ReadAllAsync([new(topic, new OffsetRange(0, 1), 0), new(topic, new OffsetRange(0, 1), 1), new(topic, new OffsetRange(0, 1), 2)],
            topic, Mock.Of<IDataFilter<string>>(), Mock.Of<IDataFilter<string>>(), cts.Token);
        try
        {
            Func<Task> act = () => pending.WaitAsync(TimeSpan.FromSeconds(5));
            (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
            closed.Should().BeTrue();
            partitionReader.Verify(x => x.Read(It.Is<PartitionWatermark>(w => w.Partition == 2), topic,
                It.IsAny<IDataFilter<string>>(), It.IsAny<IDataFilter<string>>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        finally
        {
            await cts.CancelAsync();
            _ = await Record.ExceptionAsync(() => pending);
        }
    }
    [Theory(DisplayName = "An empty buffered or first-match read honors prior cancellation.")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EmptyReadHonorsCancellation(bool firstOnly)
    {
        var topic = new LoadingTopic(new TopicName("orders"), false, new DateFilterRange(null, null), EncoderRules.String, null);
        var partitionReader = new Mock<IPartitionSnapshotReader<string, string>>(MockBehavior.Strict);
        var reader = new PartitionSnapshotBatchReader<string, string>(Options.Create(
            new SnapshotLoaderConfiguration { SearchSinglePartition = firstOnly }), partitionReader.Object);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        Func<Task> act = () => reader.ReadAsync([], topic, Mock.Of<IDataFilter<string>>(), Mock.Of<IDataFilter<string>>(), cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
        partitionReader.VerifyNoOtherCalls();
    }
}
