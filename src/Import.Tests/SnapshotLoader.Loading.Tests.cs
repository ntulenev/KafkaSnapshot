using Confluent.Kafka;

using FluentAssertions;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Names;
using KafkaSnapshot.Abstractions.Import;

using System.Text;

namespace KafkaSnapshot.Import.Tests;

public partial class SnapshotLoaderTests
{
    [Fact(DisplayName = "SnapshotLoader can load data without compacting.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanLoadDataWithoutCompacting()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var consumeCalls = 0;
        var sortCalls = 0;
        var encodeCalls = 0;
        var topicLoaderCalls = 0;
        var assignCalls = 0;
        var disposeCalls = 0;
        var closeCalls = 0;

        var consumerData = new[]
        {
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key1",
                    Value = Encoding.UTF8.GetBytes("value1"),
                    Timestamp = Timestamp.Default
                },
                Offset = new Offset(0)
            },
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key2",
                    Value = Encoding.UTF8.GetBytes("value2"),
                    Timestamp = Timestamp.Default
                },
                Offset = new Offset(1)
            },
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key2",
                    Value = Encoding.UTF8.GetBytes("value3"),
                    Timestamp = Timestamp.Default
                },
                Offset = new Offset(2)
            }
        };

        var loggerMock = CreateLoggerMock();
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration());
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(It.IsAny<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>()))
            .Callback(() => sortCalls++)
            .Returns<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>(items => items.ToList());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), EncoderRules.String))
            .Callback(() => encodeCalls++)
            .Returns<byte[], EncoderRules>((valueBytes, _) => Encoding.UTF8.GetString(valueBytes));

        var loader = CreateLoader(
            loggerMock.Object,
            optionsMock.Object,
            consumerFactory,
            topicLoaderMock.Object,
            sorterMock.Object,
            encoderMock.Object);

        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(
            new TopicName("test"),
            false,
            new DateFilterRange(null!, null!),
            EncoderRules.String,
            partitionFilter);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(
        [
            new PartitionWatermark(topicName, offset, partition)
        ]);
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, token))
            .Callback(() => topicLoaderCalls++)
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(item => item.Topic == topicName.Value.Name && item.Partition == partition)))
            .Callback(() => assignCalls++);
        consumerMock.Setup(x => x.Consume(token))
            .Returns(() => consumerData[consumeCalls++]);
        consumerMock.Setup(x => x.Dispose())
            .Callback(() => disposeCalls++);
        consumerMock.Setup(x => x.Close())
            .Callback(() => closeCalls++);

        // Act
        var result = (await loader.LoadSnapshotAsync(topicName, keyFilterMock.Object, valueFilterMock.Object, token))
            .ToList();

        // Assert
        result.Should().HaveCount(3);
        result[0].Key.Should().Be("key1");
        result[0].Value.Message.Should().Be("value1");
        result[1].Key.Should().Be("key2");
        result[1].Value.Message.Should().Be("value2");
        result[2].Key.Should().Be("key2");
        result[2].Value.Message.Should().Be("value3");
        sortCalls.Should().Be(1);
        topicLoaderCalls.Should().Be(1);
        assignCalls.Should().Be(1);
        consumeCalls.Should().Be(3);
        encodeCalls.Should().Be(3);
        closeCalls.Should().Be(1);
        disposeCalls.Should().Be(1);
    }

    [Fact(DisplayName = "SnapshotLoader can load data without compacting on date.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanLoadDataWithoutCompactingOnDate()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var consumeCalls = 0;
        var sortCalls = 0;
        var encodeCalls = 0;
        var topicLoaderCalls = 0;
        var offsetsForTimesCalls = 0;
        var assignCalls = 0;
        var disposeCalls = 0;
        var closeCalls = 0;
        var timeout = TimeSpan.FromSeconds(1);
        var testDate = DateTime.UtcNow;

        var consumerData = new[]
        {
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key1",
                    Value = Encoding.UTF8.GetBytes("value1"),
                    Timestamp = Timestamp.Default
                },
                Offset = new Offset(0)
            },
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key2",
                    Value = Encoding.UTF8.GetBytes("value2"),
                    Timestamp = Timestamp.Default
                },
                Offset = new Offset(1)
            },
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key2",
                    Value = Encoding.UTF8.GetBytes("value3"),
                    Timestamp = Timestamp.Default
                },
                Offset = new Offset(2)
            }
        };

        var loggerMock = CreateLoggerMock();
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration
        {
            DateOffsetTimeout = timeout
        });
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(It.IsAny<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>()))
            .Callback(() => sortCalls++)
            .Returns<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>(items => items.ToList());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), EncoderRules.String))
            .Callback(() => encodeCalls++)
            .Returns<byte[], EncoderRules>((valueBytes, _) => Encoding.UTF8.GetString(valueBytes));

        var loader = CreateLoader(
            loggerMock.Object,
            optionsMock.Object,
            consumerFactory,
            topicLoaderMock.Object,
            sorterMock.Object,
            encoderMock.Object);

        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(
            new TopicName("test"),
            false,
            new DateFilterRange(testDate, null!),
            EncoderRules.String,
            partitionFilter);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var expectedTimestamp = new Timestamp(testDate).UnixTimestampMs;
        var topicWatermark = new TopicWatermark(
        [
            new PartitionWatermark(topicName, offset, partition)
        ]);
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, token))
            .Callback(() => topicLoaderCalls++)
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.OffsetsForTimes(
                It.Is<IEnumerable<TopicPartitionTimestamp>>(items =>
                    items.Count() == 1
                    && items.Single().Topic == topicName.Value.Name
                    && items.Single().Partition == partition
                    && items.Single().Timestamp.UnixTimestampMs == expectedTimestamp),
                timeout))
            .Callback(() => offsetsForTimesCalls++)
            .Returns(
            [
                new TopicPartitionOffset(topicPartition, new Offset(0))
            ]);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartitionOffset>(item =>
                item.Topic == topicName.Value.Name
                && item.Partition == partition
                && item.Offset == new Offset(0))))
            .Callback(() => assignCalls++);
        consumerMock.Setup(x => x.Consume(token))
            .Returns(() => consumerData[consumeCalls++]);
        consumerMock.Setup(x => x.Dispose())
            .Callback(() => disposeCalls++);
        consumerMock.Setup(x => x.Close())
            .Callback(() => closeCalls++);

        // Act
        var result = (await loader.LoadSnapshotAsync(topicName, keyFilterMock.Object, valueFilterMock.Object, token))
            .ToList();

        // Assert
        result.Should().HaveCount(3);
        result[0].Key.Should().Be("key1");
        result[0].Value.Message.Should().Be("value1");
        result[1].Key.Should().Be("key2");
        result[1].Value.Message.Should().Be("value2");
        result[2].Key.Should().Be("key2");
        result[2].Value.Message.Should().Be("value3");
        sortCalls.Should().Be(1);
        topicLoaderCalls.Should().Be(1);
        offsetsForTimesCalls.Should().Be(1);
        assignCalls.Should().Be(1);
        consumeCalls.Should().Be(3);
        encodeCalls.Should().Be(3);
        closeCalls.Should().Be(1);
        disposeCalls.Should().Be(1);
    }

    [Fact(DisplayName = "SnapshotLoader returns empty snapshot without watermarks.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderReturnsEmptySnapshotWithoutWatermarks()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var consumerFactoryCalls = 0;
        Func<IConsumer<object, byte[]>> consumerFactory = () =>
        {
            consumerFactoryCalls++;
            throw new InvalidOperationException("Consumer should not be created without watermarks.");
        };

        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration());
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(It.IsAny<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>()))
            .Returns<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>(items => items.ToList());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var loader = CreateLoader(
            loggerMock.Object,
            optionsMock.Object,
            consumerFactory,
            topicLoaderMock.Object,
            sorterMock.Object,
            encoderMock.Object);
        var topic = new LoadingTopic(
            new TopicName("test"),
            false,
            new DateFilterRange(null!, null!),
            EncoderRules.String,
            null);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(
            consumerFactory,
            topic,
            token)).ReturnsAsync(new TopicWatermark([]));

        // Act
        var result = (await loader.LoadSnapshotAsync(
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token)).ToList();

        // Assert
        result.Should().BeEmpty();
        consumerFactoryCalls.Should().Be(0);
    }

    [Fact(DisplayName = "SnapshotLoader searches single partitions until first match.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderSearchesSinglePartitionsUntilFirstMatch()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var consumers = new Queue<IConsumer<object, byte[]>>();
        var consumerFactoryCalls = 0;
        Func<IConsumer<object, byte[]>> consumerFactory = () =>
        {
            consumerFactoryCalls++;
            return consumers.Dequeue();
        };

        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration
        {
            SearchSinglePartition = true
        });
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(It.IsAny<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>()))
            .Returns<IEnumerable<KeyValuePair<object, KafkaMessage<object>>>>(items => items.ToList());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), EncoderRules.String))
            .Returns<byte[], EncoderRules>((valueBytes, _) => Encoding.UTF8.GetString(valueBytes));

        var loader = CreateLoader(
            loggerMock.Object,
            optionsMock.Object,
            consumerFactory,
            topicLoaderMock.Object,
            sorterMock.Object,
            encoderMock.Object);
        var topic = new LoadingTopic(
            new TopicName("test"),
            false,
            new DateFilterRange(null!, null!),
            EncoderRules.String,
            null);
        var skippedPartition = new Partition(1);
        var matchedPartition = new Partition(2);
        var untouchedPartition = new Partition(3);
        var topicWatermark = new TopicWatermark(
        [
            new PartitionWatermark(topic, new WatermarkOffsets(new Offset(0), new Offset(1)), skippedPartition),
            new PartitionWatermark(topic, new WatermarkOffsets(new Offset(0), new Offset(1)), matchedPartition),
            new PartitionWatermark(topic, new WatermarkOffsets(new Offset(0), new Offset(1)), untouchedPartition)
        ]);
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(
            consumerFactory,
            topic,
            token)).ReturnsAsync(topicWatermark);

        consumers.Enqueue(CreateSingleMessageConsumer(topic, skippedPartition, "skipped", "value", token));
        consumers.Enqueue(CreateSingleMessageConsumer(topic, matchedPartition, "matched", "value", token));
        consumers.Enqueue(CreateSingleMessageConsumer(topic, matchedPartition, "matched", "value", token));

        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.SetupSequence(x => x.IsMatch(It.IsAny<object>()))
            .Returns(false)
            .Returns(true)
            .Returns(true);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

        // Act
        var result = (await loader.LoadSnapshotAsync(
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token)).ToList();

        // Assert
        result.Should().ContainSingle();
        result[0].Key.Should().Be("matched");
        result[0].Value.Message.Should().Be("value");
        result[0].Value.Meta.Partition.Should().Be(matchedPartition.Value);
        consumerFactoryCalls.Should().BeInRange(2, 3);
    }
}
