using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Unit")]
public class SnapshotLoaderStreamingTests
{
    [Theory(DisplayName = "Streaming buffers snapshots only when sorting or compaction requires it.")]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    public async Task BufferingIsSelectedByDomainOperation(bool compacting, bool sorting)
    {
        var topic = new LoadingTopic(new TopicName("orders"), compacting, new DateFilterRange(null, null), EncoderRules.String, null);
        var metadata = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var watermark = new TopicWatermark([]);
        metadata.Setup(x => x.LoadWatermarksAsync(topic, It.IsAny<CancellationToken>())).ReturnsAsync(watermark);
        var batch = new Mock<IPartitionSnapshotBatchReader<string, string>>(MockBehavior.Strict);
        var key = Mock.Of<IDataFilter<string>>();
        var value = Mock.Of<IDataFilter<string>>();
        KeyValuePair<string, KafkaMessage<string>>[] items = [new("key", new("1", new(DateTimeOffset.UnixEpoch, 0, 0)))];
        var sorter = new Mock<IMessageSorter<string, string>>(MockBehavior.Strict);
        sorter.SetupGet(x => x.IsEnabled).Returns(sorting);
        sorter.Setup(x => x.Sort(It.IsAny<IEnumerable<KeyValuePair<string, KafkaMessage<string>>>>())).Returns(items);
        if (compacting || sorting)
        {
            batch.Setup(x => x.ReadAsync(watermark.Watermarks, topic, key, value, It.IsAny<CancellationToken>())).ReturnsAsync(items);
        }
        else
        {
            batch.Setup(x => x.StreamAsync(watermark.Watermarks, topic, key, value, It.IsAny<CancellationToken>())).Returns(Source(items));
        }

        var builder = new SnapshotBuilder<string, string>(NullLogger<SnapshotBuilder<string, string>>.Instance, sorter.Object);
        var loader = new SnapshotLoader<string, string>(NullLogger<SnapshotLoader<string, string>>.Instance, metadata.Object, batch.Object, builder);
        var result = new List<KeyValuePair<string, KafkaMessage<string>>>();
        await foreach (var item in loader.StreamSnapshotAsync(topic, key, value, CancellationToken.None))
        {
            result.Add(item);
        }

        result.Should().Equal(items);
        metadata.VerifyAll();
        batch.VerifyAll();
        batch.VerifyNoOtherCalls();
    }

    private static async IAsyncEnumerable<T> Source<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }
}
