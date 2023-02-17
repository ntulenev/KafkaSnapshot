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

namespace KafkaSnapshot.Import.Tests;

public class SnapshotLoaderTests
{
    [Fact(DisplayName = "SnapshotLoader can't be created with null logger.")]
    [Trait("Category", "Unit")]
    public void CantCreateSnapshotLoaderWithNulLogger()
    {

        // Arrange
        var logger = (ILogger<SnapshotLoader<object, object>>)null!;
        IConsumer<object, object> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter));

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

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, null!, topicLoader, sorter));

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
        IConsumer<object, object> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, null!, consumerFactory, topicLoader, sorter));

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
        IConsumer<object, object> consumerFactory() => throw new NotImplementedException();
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, null!, sorter));

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
        IConsumer<object, object> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, null!));

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
        IConsumer<object, object> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;

        // Act
        var exception = Record.Exception(
            () => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter));

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
        IConsumer<object, object> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var filterKeyMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var keyFilter = filterKeyMock.Object;
        var filterValueMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilter = filterValueMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadCompactSnapshotAsync(
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
        IConsumer<object, object> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicParams = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!), partitionFilter);
        var filterValueMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilter = filterValueMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadCompactSnapshotAsync(
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
        IConsumer<object, object> consumerFactory() => throw new NotImplementedException();
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicParams = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!), partitionFilter);
        var filterKeyMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var keyFilter = filterKeyMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadCompactSnapshotAsync(
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
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = false;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!), partitionFilter);
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
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadCompactSnapshotAsync(
                topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

        // Assert
        result.Should().BeEquivalentTo(exceptedData);
        dispCount.Should().Be(1);
        closeCount.Should().Be(1);
    }

    //if (_config.SearchSinglePartition)
    //if (topicParams.HasOffsetDate)
    //isFinalOffsetDateReached
    //if (keyFilter.IsMatch(result.Message.Key) && valueFilter.IsMatch(result.Message.Value))


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
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = false;
        var testDate = DateTime.UtcNow;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(testDate, null!), partitionFilter);
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
        var topicPartition = new TopicPartition(topicName.Value, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
            It.IsAny<TimeSpan>())).Returns(new[]
            {
                new TopicPartitionOffset(topicPartition,new Offset(0))
            }.ToList());
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartitionOffset>(x => x.Topic == topicName.Value && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

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
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var emptyData = Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>();
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(emptyData)).Returns(emptyData);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = compacting;
        var testDate = DateTime.UtcNow;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(testDate, null!), partitionFilter);
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
        var topicPartition = new TopicPartition(topicName.Value, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
            It.IsAny<TimeSpan>())).Returns(new[]
            {
                new TopicPartitionOffset(topicPartition,new Offset(Offset.End))
            }.ToList());
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

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
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!), partitionFilter);
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
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

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
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!), partitionFilter);
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
        var topicPartition = new TopicPartition(topicName.Value, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
            It.IsAny<TimeSpan>())).Returns(new[]
            {
                new TopicPartitionOffset(topicPartition,new Offset(0))
            }.ToList());
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value && x.Partition == partition)));
        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false);

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
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>(MockBehavior.Strict);
        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>(MockBehavior.Strict);
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;
        var sorterMock = new Mock<IMessageSorter<object, object>>(MockBehavior.Strict);
        var empty = Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>();
        sorterMock.Setup(x => x.Sort(empty)).Returns(empty);
        var sorter = sorterMock.Object;
        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = compacting;
        var testDate = DateTime.UtcNow;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, testDate), partitionFilter);
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
        var topicPartition = new TopicPartition(topicName.Value, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));
        consumerMock.Setup(x => x.Consume(It.IsAny<CancellationToken>())).Returns(new ConsumeResult<object, object>
        {
            Message = new Message<object, object>
            {
                Timestamp = new Timestamp(testDate.AddDays(1))
            }
        });
        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(x => x.Topic == topicName.Value && x.Partition == partition)));
        var dispCount = 0;
        var closeCount = 0;
        consumerMock.Setup(x => x.Dispose()).Callback(() => dispCount++);
        consumerMock.Setup(x => x.Close()).Callback(() => closeCount++);

        // Act
        var result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None)
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
        var consumerMock = new Mock<IConsumer<object, object>>();
        Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
        var topicLoaderMock = new Mock<ITopicWatermarkLoader>();

        var topicLoader = topicLoaderMock.Object;
        var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
        optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
        var options = optionsMock.Object;

        var sorterMock = new Mock<IMessageSorter<object, object>>();
        sorterMock.Setup(x => x.Sort(exceptedData)).Returns(exceptedData);
        var sorter = sorterMock.Object;

        var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader, sorter);
        var withCompacting = false;

        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, testDate), partitionFilter);

        var keyFilterMock = new Mock<IDataFilter<object>>();
        keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var keyfilter = keyFilterMock.Object;

        var valueFilterMock = new Mock<IDataFilter<object>>();
        valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
        var valueFilter = valueFilterMock.Object;

        IEnumerable<KeyValuePair<object, KafkaMessage<object>>> result = null!;
        var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
        var partition = new Partition(1);
        var topicWatermark = new TopicWatermark(new[]
        {
            new PartitionWatermark(topicName,offset,partition)
        });

        var topicPartition = new TopicPartition(topicName.Value, partition);
        var partitionWithTime = new TopicPartitionTimestamp(topicPartition, new Timestamp(testDate));

        consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
        {
            return consumerData[indexer++];
        });

        topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
            .Returns(Task.FromResult(topicWatermark));

        // Act
        var exception = await Record.ExceptionAsync(
            async () => result = await loader.LoadCompactSnapshotAsync(topicName, keyfilter, valueFilter, CancellationToken.None).ConfigureAwait(false));

        // Assert
        exception.Should().BeNull();
        result.Should().BeEquivalentTo(exceptedData);
    }
}
