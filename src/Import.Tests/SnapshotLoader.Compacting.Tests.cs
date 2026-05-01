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
    [Fact(DisplayName = "SnapshotLoader can load data with compacting.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanLoadDataWithCompacting()
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
                       Value = Encoding.UTF8.GetBytes("value1"),
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(0)
            },
            new ConsumeResult<object, byte[]>
            {
                 Message = new Message<object, byte[]>
                 {
                       Key = "key2",
                       Value = Encoding.UTF8.GetBytes("value2"),
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(1)
            },
            new ConsumeResult<object, byte[]>
            {
                 Message = new Message<object, byte[]>
                 {
                       Key = "key2",
                       Value = Encoding.UTF8.GetBytes("value3"),
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

    [Fact(DisplayName = "SnapshotLoader can create compacting snapshot with debug logging.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanCreateCompactingSnapshotWithDebugLogging()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        loggerMock.Setup(x => x.IsEnabled(LogLevel.Debug)).Returns(true);
        var logger = loggerMock.Object;

        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;

        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration());

        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), EncoderRules.String)).Returns("encoded");

        var loader = CreateLoader(
            logger,
            optionsMock.Object,
            consumerFactory,
            topicLoaderMock.Object,
            sorterMock.Object,
            encoderMock.Object);

        var topic = new LoadingTopic(
            new TopicName("test"),
            true,
            new DateFilterRange(null!, null!),
            EncoderRules.String,
            null);

        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(
        [
            new PartitionWatermark(topic, new WatermarkOffsets(new Offset(0), new Offset(1)), partition)
        ]);
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(
            consumerFactory,
            topic,
            token)).ReturnsAsync(topicWatermark);

        consumerMock.Setup(x => x.Assign(It.IsAny<TopicPartition>()));
        consumerMock.Setup(x => x.Consume(token)).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Key = "k",
                Value = Encoding.UTF8.GetBytes("v"),
                Timestamp = Timestamp.Default
            },
            Offset = new Offset(0)
        });
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());

        // Act
        var result = (await loader.LoadSnapshotAsync(
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token)).ToList();

        // Assert
        result.Should().HaveCount(1);
    }

}
