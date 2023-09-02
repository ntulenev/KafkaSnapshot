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

namespace KafkaSnapshot.Import.Tests;

public class SnapshotLoaderTests
{
    [Fact(DisplayName = "SnapshotLoader can't be created with null logger.")]
    [Trait("Category", "Unit")]
    public void CantCreateSnapshotLoaderWithNulLogger()
    {

        // Arrange
        var logger = (ILogger<SnapshotLoader<object, object>>)null!;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null factory.")]
    [Trait("Category", "Unit")]
    public void CantCreateSnapshotLoaderWithNullFactory()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, null!, topicLoader, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null options.")]
    [Trait("Category", "Unit")]
    public void CantCreateSnapshotLoaderWithNullOptions()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, null!, consumerFactory, topicLoader, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't be created with null topic loader.")]
    [Trait("Category", "Unit")]
    public void CantCreateSnapshotLoaderWithNullTopicLoader()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, null!, sorter, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader cant be created with null sorter.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderCantBeCreatedWithNullSorter()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>(MockBehavior.Strict);
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, null!, encoder));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader cant be created with null encoder.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderCantBeCreatedWithNullEncoder()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>(MockBehavior.Strict);
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can be created with valid params.")]
    [Trait("Category", "Unit")]
    public void SnapshotLoaderCanBeCreated()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "SnapshotLoader can't load for null topic.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCantLoadForNullTopic()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var filterKeyMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var keyFilter = filterKeyMock.Object;
        var filterValueMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilter = filterValueMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadSnapshotAsync(
                null!, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));


        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't load for null key filter.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCantLoadForNullKeyFilter()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicParams = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var filterValueMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilter = filterValueMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadSnapshotAsync(
                topicParams, null!, valueFilter, CancellationToken.None).ConfigureAwait(false));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }


    [Fact(DisplayName = "SnapshotLoader can't load for null value filter.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCantLoadForNullValueFilter()
    {

        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        IConsumer<object, byte[]> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicParams = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var filterKeyMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var keyFilter = filterKeyMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadSnapshotAsync(
                topicParams, keyFilter, null!, CancellationToken.None).ConfigureAwait(false));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can load data without compacting.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanLoadDataWithoutCompacting()
    {

        // Arrange
        var consumerData = new[]
       {
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key1",
                       Value = "value1",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(0)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(1)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(2)
            }
        };
        var indexer = 0;
        var exceptedData = consumerData.Select((x, i) =>
        new KeyValuePair<object, KafkaMessage<object>>(
            x.Message.Key, new KafkaMessage<object>(
                x.Message.Value, new Models.Message.KafkaMetadata(
                    x.Message.Timestamp.UtcDateTime, 1, i)))
        );
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = false;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var filterKeyMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        filterKeyMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var keyFilter = filterKeyMock.Object;
        var filterValueMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        filterValueMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilter = filterValueMock.Object;
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(new[]
        {
            new PartitionWatermark(topicName,offset,partition)
        });
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value.Name && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(
                topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

        // Assert
        result.Should().BeEquivalentTo(exceptedData);
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }

    [Fact(DisplayName = "SnapshotLoader can load data without compacting on date.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanLoadDataWithoutCompactingOnDate()
    {
        // Arrange
        var indexer = 0;
        var consumerData = new[]
        {
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key1",
                       Value = "value1",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(0)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(1)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(2)
            }
        };
        var exceptedData = consumerData.Select((x, i) =>
                        new KeyValuePair<object, KafkaMessage<object>>(
                            x.Message.Key, new KafkaMessage<object>(
                                x.Message.Value, new Models.Message.KafkaMetadata(x.Message.Timestamp.UtcDateTime, 1, i))));
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = false;
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
        var topicWatermark = new TopicWatermark(new[]
        {
            new PartitionWatermark(topicName,offset,partition)
        });
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
            It.IsAny<TimeSpan>())).Returns(new[]
            {
                new TopicPartitionOffset(topicPartition,new Offset(0))
            }.ToList());
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartitionOffset>(x => x.Topic == topicName.Value.Name && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

        // Assert
        result.Should().BeEquivalentTo(exceptedData);
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }

    [Theory(DisplayName = "SnapshotLoader skips load data on out of range date.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SnapshotLoaderSkipsLoadDataOnNonRangeDate(bool compacting)
    {
        // Arrange
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
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
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
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
        var topicWatermark = new TopicWatermark(new[]
        {
            new PartitionWatermark(topicName,offset,partition)
        });
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
            It.IsAny<TimeSpan>())).Returns(new[]
            {
                new TopicPartitionOffset(topicPartition,new Offset(Offset.End))
            }.ToList());
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

        // Assert
        result.Should().BeEquivalentTo(Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>());
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }

    [Fact(DisplayName = "SnapshotLoader can load data with compacting.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanLoadDataWithCompacting()
    {
        // Arrange
        var indexer = 0;
        var consumerData = new[]
        {
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key1",
                       Value = "value1",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(0)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(1)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(2)
            }
        };
        var exceptedData = consumerData.Select((x, i) =>
        new KeyValuePair<object, KafkaMessage<object>>(x.Message.Key, new KafkaMessage<object>(x.Message.Value, new KafkaMetadata(x.Message.Timestamp.UtcDateTime, 1, i)))
        ).ToList();
        exceptedData.RemoveAt(1);
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var keyFilter = keyFilterMock.Object;
        var valueFilterMock = new Mock<IDataFilter<object>>();
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilter = valueFilterMock.Object;
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(new[]
        {
            new PartitionWatermark(topicName,offset,partition)
        });
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value.Name && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

        // Assert
        result.Should().BeEquivalentTo(exceptedData);
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }

    [Fact(DisplayName = "SnapshotLoader can load data with compacting on date.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCanLoadDataWithCompactingOnDate()
    {
        // Arrange
        var indexer = 0;
        var consumerData = new[]
        {
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key1",
                       Value = "value1",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(0)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(1)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  Timestamp.Default
                 },
                 Offset = new Offset(2)
            }
        };
        var exceptedData = consumerData.Select((x, i) =>
        new KeyValuePair<object, KafkaMessage<object>>(x.Message.Key, new KafkaMessage<object>(x.Message.Value, new KafkaMetadata(x.Message.Timestamp.UtcDateTime, 1, i)))
        ).ToList();
        exceptedData.RemoveAt(1);
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
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
        var topicWatermark = new TopicWatermark(new[]
        {
            new PartitionWatermark(topicName,offset,partition)
        });
        var testDate = DateTime.UtcNow;
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
            It.IsAny<TimeSpan>())).Returns(new[]
            {
                new TopicPartitionOffset(topicPartition,new Offset(0))
            }.ToList());
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value.Name && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

        // Assert
        result.Should().BeEquivalentTo(exceptedData);
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
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
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
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
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
        var topicWatermark = new TopicWatermark(new[]
        {
            new PartitionWatermark(topicName,offset,partition)
        });
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.Consume(It.IsAny<CancellationToken>())).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Timestamp = new Timestamp(testDate.AddDays(1))
            }
        });
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value.Name && x.Partition == partition)));
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None)
            .ConfigureAwait(false);

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
        var indexer = 0;
        var testDate = DateTime.UtcNow;
        var consumerData = new[]
        {
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key1",
                       Value = "value1",
                       Timestamp =  new Timestamp(testDate.AddDays(-1))
                 },
                 Offset = new Offset(0)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  new Timestamp(testDate)
                 },
                 Offset = new Offset(1)
            },
            new ConsumeResult<object, object>
            {
                 Message = new Message<object, object>
                 {
                       Key = "key2",
                       Value = "value2",
                       Timestamp =  new Timestamp(testDate.AddDays(+1))
                 },
                 Offset = new Offset(2)
            }
        };
        var exceptedData = consumerData.Select((x, i) =>
        new KeyValuePair<object, KafkaMessage<object>>(x.Message.Key, new KafkaMessage<object>(x.Message.Value, new KafkaMetadata(x.Message.Timestamp.UtcDateTime, 1, i)))
        ).ToList();
        exceptedData.RemoveAt(2);
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        var logger = loggerMock.Object;
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        Func<IConsumer<object, byte[]>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), It.IsAny<EncoderRules>()));
        var encoder = encoderMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = false;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, testDate), EncoderRules.String, partitionFilter);
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var keyfilter = keyFilterMock.Object;
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilter = valueFilterMock.Object;
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(new[]
        {
            new PartitionWatermark(topicName,offset,partition)
        });
        var topicPartition = new TopicPartition(topicName.Value.Name, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, byte[]>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value.Name && x.Partition == partition)));
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadSnapshotAsync(topicName, keyfilter, valueFilter, CancellationToken.None)
            .ConfigureAwait(false);

        // Assert
        result.Should().BeEquivalentTo(exceptedData);
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }
}
