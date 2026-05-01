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
    [Theory(DisplayName = "SnapshotLoader skips load data on out of range date.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SnapshotLoaderSkipsLoadDataOnNonRangeDate(bool compacting)
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var logger = loggerMock.Object;
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var emptyData = Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>();
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(emptyData)).Returns(emptyData);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = compacting;
        var testDate = DateTime.UtcNow;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(testDate, null!), EncoderRules.String, partitionFilter);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var keyFilter = keyFilterMock.Object;
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilter = valueFilterMock.Object;
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(
        [
            new PartitionWatermark(topicName,offset,partition)
        ]);
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
            It.IsAny<TimeSpan>())).Returns(
            [
                new TopicPartitionOffset(topicPartition,new Offset(Offset.End))
            ]);
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, token))
            .Returns(Task.FromResult(topicWatermark));
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyFilter, valueFilter, token);

        // Assert
        result.Should().BeEquivalentTo(Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>());
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }

    [Fact(DisplayName = "SnapshotLoader can load data with compacting on date.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanLoadDataWithCompactingOnDate()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var indexer = 0;
        var consumerData = new[]
        {
            new ConsumeResult<object, byte[]>
            {
                 Message = new Message<object, byte[]>
                 {
                       Key = "key1",
                       Value = Encoding.UTF8.GetBytes("Test1"),
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(0)
            },
            new ConsumeResult<object, byte[]>
            {
                 Message = new Message<object, byte[]>
                 {
                       Key = "key2",
                       Value = Encoding.UTF8.GetBytes("Test2"),
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(1)
            },
            new ConsumeResult<object, byte[]>
            {
                 Message = new Message<object, byte[]>
                 {
                       Key = "key2",
                       Value = Encoding.UTF8.GetBytes("Test3"),
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(2)
            }
        };
        var expectedData = consumerData.Select((x, i) =>
        new KeyValuePair<object, KafkaMessage<object>>(
            x.Message.Key,
            new KafkaMessage<object>(
                Encoding.UTF8.GetString(x.Message.Value),
                new KafkaMetadata(x.Message.Timestamp.UtcDateTime, 1, i))))
        .ToList();
        expectedData.RemoveAt(1);
        var loggerMock = CreateLoggerMock();
        var logger = loggerMock.Object;
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(expectedData)).Returns(expectedData);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()))
            .Returns<byte[], EncoderRules>((bytes, _) => Encoding.UTF8.GetString(bytes));
        var encoder = encoderMock.Object;
        var loader = CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

        var keyFilter = keyFilterMock.Object;
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilter = valueFilterMock.Object;
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(
        [
            new PartitionWatermark(topicName,offset,partition)
        ]);
        var testDate = DateTime.UtcNow;
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
            It.IsAny<TimeSpan>())).Returns(
            [
                new TopicPartitionOffset(topicPartition,new Offset(0))
            ]);
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, token))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value.Name && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Returns(() => consumerData[indexer++]);
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyFilter, valueFilter, token);

        // Assert
        result.Should().BeEquivalentTo(expectedData);
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }

    [Theory(DisplayName = "SnapshotLoader skips load data on out of range end date.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SnapshotLoaderLoadNoDataWithEndDate(bool compacting)
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var logger = loggerMock.Object;
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var empty = Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>();
        sorterMock.Setup(x => x.Sort(empty)).Returns(empty);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>())).Returns("test");

        var encoder = encoderMock.Object;
        var loader = CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = compacting;
        var testDate = DateTime.UtcNow;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, testDate), EncoderRules.String, partitionFilter);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var keyFilter = keyFilterMock.Object;
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilter = valueFilterMock.Object;
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(
        [
            new PartitionWatermark(topicName,offset,partition)
        ]);
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.Consume(It.Is<CancellationToken>(currentToken => currentToken == token))).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Timestamp = new Timestamp(testDate.AddDays(1))
            }
        });
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, token))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value.Name && x.Partition == partition)));
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyFilter, valueFilter, token);

        // Assert
        result.Should().BeEquivalentTo(Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>());
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }

    [Fact(DisplayName = "SnapshotLoader load data with end date filter.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderLoadDataWithEndDate()
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
        var endDate = DateTime.UtcNow;

        var consumerData = new[]
        {
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key1",
                    Value = Encoding.UTF8.GetBytes("value1"),
                    Timestamp = new Timestamp(endDate.AddDays(-1))
                },
                Offset = new Offset(0)
            },
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key2",
                    Value = Encoding.UTF8.GetBytes("value2"),
                    Timestamp = new Timestamp(endDate)
                },
                Offset = new Offset(1)
            },
            new ConsumeResult<object, byte[]>
            {
                Message = new Message<object, byte[]>
                {
                    Key = "key2",
                    Value = Encoding.UTF8.GetBytes("value3"),
                    Timestamp = new Timestamp(endDate.AddDays(1))
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
            new DateFilterRange(null!, endDate),
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
        result.Should().HaveCount(2);
        result[0].Key.Should().Be("key1");
        result[0].Value.Message.Should().Be("value1");
        result[1].Key.Should().Be("key2");
        result[1].Value.Message.Should().Be("value2");
        sortCalls.Should().Be(1);
        topicLoaderCalls.Should().Be(1);
        assignCalls.Should().Be(1);
        consumeCalls.Should().Be(3);
        encodeCalls.Should().Be(2);
        closeCalls.Should().Be(1);
        disposeCalls.Should().Be(1);
    }
}
