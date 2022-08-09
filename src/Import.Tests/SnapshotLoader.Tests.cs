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
using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Import.Tests
{
    public class SnapshotLoaderTests
    {
        [Fact(DisplayName = "SnapshotLoader can't be created with null logger.")]
        [Trait("Category", "Unit")]
        public void CantCreateSnapshotLoaderWithNulLogger()
        {

            // Arrange
            var logger = (ILogger<SnapshotLoader<object, object>>)null!;
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;

            // Act
            var exception = Record.Exception(() => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader));

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
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;

            // Act
            var exception = Record.Exception(() => new SnapshotLoader<object, object>(logger, options, null!, topicLoader));

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
            IConsumer<object, object> consumerFactory() => null!;
            ITopicWatermarkLoader topicLoader = null!;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;

            // Act
            var exception = Record.Exception(() => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader));

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
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;

            // Act
            var exception = Record.Exception(() => new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader));

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
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var topicName = (LoadingTopic)null!;

            var filterKeyMock = new Mock<IDataFilter<object>>();
            var keyFilter = filterKeyMock.Object;

            var filterValueMock = new Mock<IDataFilter<object>>();
            var valueFilter = filterValueMock.Object;

            // Act
            var exception = await Record.ExceptionAsync(
                async () => _ = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));


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
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = true;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicParams = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!), partitionFilter, sort);
            var keyFilter = (IDataFilter<object>)null!;


            var filterValueMock = new Mock<IDataFilter<object>>();
            var valueFilter = filterValueMock.Object;

            // Act
            var exception = await Record.ExceptionAsync(
                async () => _ = await loader.LoadCompactSnapshotAsync(topicParams, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));


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
            IConsumer<object, object> consumerFactory() => null!;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();
            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = true;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicParams = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!), partitionFilter, sort);

            var filterKeyMock = new Mock<IDataFilter<object>>();
            var keyFilter = filterKeyMock.Object;

            var valueFilter = (IDataFilter<object>)null!;

            // Act
            var exception = await Record.ExceptionAsync(
                async () => _ = await loader.LoadCompactSnapshotAsync(topicParams, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));


            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SnapshotLoader can load data without compacting.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderCanLoadDataWithoutCompacting()
        {

            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            var consumerMock = new Mock<IConsumer<object, object>>();
            Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();

            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = false;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!), partitionFilter, sort);
            var filterKeyMock = new Mock<IDataFilter<object>>();
            filterKeyMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

            var keyFilter = filterKeyMock.Object;

            var filterValueMock = new Mock<IDataFilter<object>>();
            filterValueMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
            var valueFilter = filterValueMock.Object;

            IEnumerable<KeyValuePair<object, KafkaMessage<object>>> result = null!;
            var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
            var topicWatermark = new TopicWatermark(new[]
            {
                new PartitionWatermark(topicName,offset,new Partition(1))
            });

            topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
                .Returns(Task.FromResult(topicWatermark));

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

            new KeyValuePair<object, KafkaMessage<object>>(x.Message.Key, new KafkaMessage<object>(x.Message.Value, new Models.Message.KafkaMetadata(x.Message.Timestamp.UtcDateTime, 1, i)))

            );

            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(exceptedData);
        }

        [Fact(DisplayName = "SnapshotLoader can load data without compacting on date.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderCanLoadDataWithoutCompactingOnDate()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            var consumerMock = new Mock<IConsumer<object, object>>();
            Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();

            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = false;
            var testDate = DateTime.UtcNow;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(testDate, null!), partitionFilter, sort);

            var keyFilterMock = new Mock<IDataFilter<object>>();
            keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
            var keyFilter = keyFilterMock.Object;

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
            consumerMock.Setup(x => x.OffsetsForTimes(
                It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
                It.IsAny<TimeSpan>())).Returns(new[]
                {
                    new TopicPartitionOffset(topicPartition,new Offset(0))
                }.ToList());

            topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
                .Returns(Task.FromResult(topicWatermark));

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
                            new KeyValuePair<object, KafkaMessage<object>>(x.Message.Key, new KafkaMessage<object>(x.Message.Value, new Models.Message.KafkaMetadata(x.Message.Timestamp.UtcDateTime, 1, i))));

            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(exceptedData);
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
            var consumerMock = new Mock<IConsumer<object, object>>();
            Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();

            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = compacting;
            var testDate = DateTime.UtcNow;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(testDate, null!), partitionFilter, sort);

            var keyFilterMock = new Mock<IDataFilter<object>>();
            keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
            var keyFilter = keyFilterMock.Object;

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
            consumerMock.Setup(x => x.OffsetsForTimes(
                It.Is<IEnumerable<TopicPartitionTimestamp>>(x => x.Single() == partitionWithTime),
                It.IsAny<TimeSpan>())).Returns(new[]
                {
                    new TopicPartitionOffset(topicPartition,new Offset(Offset.End))
                }.ToList());

            topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
                .Returns(Task.FromResult(topicWatermark));

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>());
        }

        [Fact(DisplayName = "SnapshotLoader can load data with compacting.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderCanLoadDataWithCompacting()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            var consumerMock = new Mock<IConsumer<object, object>>();
            Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();

            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = true;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!),partitionFilter,sort);

            var keyFilterMock = new Mock<IDataFilter<object>>();
            keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
            var keyFilter = keyFilterMock.Object;

            var valueFilterMock = new Mock<IDataFilter<object>>();
            valueFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
            var valueFilter = valueFilterMock.Object;


            IEnumerable<KeyValuePair<object, KafkaMessage<object>>> result = null!;
            var offset = new WatermarkOffsets(new Offset(0), new Offset(3));
            var topicWatermark = new TopicWatermark(new[]
            {
                new PartitionWatermark(topicName,offset,new Partition(1))
            });

            topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
                .Returns(Task.FromResult(topicWatermark));

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


            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(exceptedData);
        }

        [Fact(DisplayName = "SnapshotLoader can load data with compacting on date.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderCanLoadDataWithCompactingOnDate()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            var consumerMock = new Mock<IConsumer<object, object>>();
            Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();

            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = true;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, null!),partitionFilter,sort);

            var keyFilterMock = new Mock<IDataFilter<object>>();
            keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
            var keyFilter = keyFilterMock.Object;

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


            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(exceptedData);
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
            var consumerMock = new Mock<IConsumer<object, object>>();
            Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();

            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = compacting;
            var testDate = DateTime.UtcNow;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, testDate),partitionFilter,sort);

            var keyFilterMock = new Mock<IDataFilter<object>>();
            keyFilterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);
            var keyFilter = keyFilterMock.Object;

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

            consumerMock.Setup(x => x.Consume(It.IsAny<CancellationToken>())).Returns(new ConsumeResult<object, object>
            {
                Message = new Message<object, object>
                {
                    Timestamp = new Timestamp(testDate.AddDays(1))
                }
            });

            topicLoaderMock.Setup(x => x.LoadWatermarksAsync<object, object>(consumerFactory, topicName, CancellationToken.None))
                .Returns(Task.FromResult(topicWatermark));

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, keyFilter, valueFilter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(Enumerable.Empty<KeyValuePair<object, KafkaMessage<object>>>());
        }

        [Fact(DisplayName = "SnapshotLoader load data with end date filter.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderLoadDataWithEndDate()
        {
            // Arrange
            var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
            var logger = loggerMock.Object;
            var consumerMock = new Mock<IConsumer<object, object>>();
            Func<IConsumer<object, object>> consumerFactory = () => consumerMock.Object;
            var topicLoaderMock = new Mock<ITopicWatermarkLoader>();

            var topicLoader = topicLoaderMock.Object;
            var optionsMock = new Mock<IOptions<SnapshotLoaderConfiguration>>();
            optionsMock.Setup(x => x.Value).Returns(new SnapshotLoaderConfiguration() { });
            var options = optionsMock.Object;
            var loader = new SnapshotLoader<object, object>(logger, options, consumerFactory, topicLoader);
            var withCompacting = false;
            var testDate = DateTime.UtcNow;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterRange(null!, testDate),partitionFilter,sort);

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

            var indexer = 0;
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
}
