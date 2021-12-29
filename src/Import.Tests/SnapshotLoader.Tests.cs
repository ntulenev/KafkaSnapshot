using Confluent.Kafka;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Import.Watermarks;
using Microsoft.Extensions.Options;
using KafkaSnapshot.Import.Configuration;

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
            var filterMock = new Mock<IKeyFilter<object>>();
            var filter = filterMock.Object;
            // Act
            var exception = await Record.ExceptionAsync(
                async () => _ = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None).ConfigureAwait(false));


            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SnapshotLoader can't load for null filter.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderCantLoadForNullFilter()
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
            var topicParams = new LoadingTopic("test", withCompacting, new DateFilterParams(null!, null!));
            var filter = (IKeyFilter<object>)null!;
            // Act
            var exception = await Record.ExceptionAsync(
                async () => _ = await loader.LoadCompactSnapshotAsync(topicParams, filter, CancellationToken.None).ConfigureAwait(false));


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
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterParams(null!, null!));
            var filterMock = new Mock<IKeyFilter<object>>();
            filterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

            var filter = filterMock.Object;
            IEnumerable<KeyValuePair<object, DatedMessage<object>>> result = null!;
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

            var exceptedData = consumerData.Select(x =>

            new KeyValuePair<object, DatedMessage<object>>(x.Message.Key, new DatedMessage<object>(x.Message.Value, x.Message.Timestamp.UtcDateTime)));

            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None).ConfigureAwait(false));

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
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterParams(testDate, null!));
            var filterMock = new Mock<IKeyFilter<object>>();
            filterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

            var filter = filterMock.Object;
            IEnumerable<KeyValuePair<object, DatedMessage<object>>> result = null!;
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

            var exceptedData = consumerData.Select(x =>
                            new KeyValuePair<object, DatedMessage<object>>(x.Message.Key, new DatedMessage<object>(x.Message.Value, x.Message.Timestamp.UtcDateTime)));

            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(exceptedData);
        }

        [Theory(DisplayName = "SnapshotLoader skips load data on out of range date.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public async Task SnapshotLoaderSkipsLoadDataWithoutCompactingOnNonRangeDate(bool compacting)
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
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterParams(testDate, null!));
            var filterMock = new Mock<IKeyFilter<object>>();
            filterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

            var filter = filterMock.Object;
            IEnumerable<KeyValuePair<object, DatedMessage<object>>> result = null!;
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
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(Enumerable.Empty<KeyValuePair<object, DatedMessage<object>>>());
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
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterParams(null!, null!));
            var filterMock = new Mock<IKeyFilter<object>>();
            filterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

            var filter = filterMock.Object;
            IEnumerable<KeyValuePair<object, DatedMessage<object>>> result = null!;
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

            var exceptedData = consumerData.Select(x =>

            new KeyValuePair<object, DatedMessage<object>>(x.Message.Key, new DatedMessage<object>(x.Message.Value, x.Message.Timestamp.UtcDateTime))
            ).ToList();
            exceptedData.RemoveAt(1);


            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None).ConfigureAwait(false));

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
            var topicName = new LoadingTopic("test", withCompacting, new DateFilterParams(null!, null!));
            var filterMock = new Mock<IKeyFilter<object>>();
            filterMock.Setup(x => x.IsMatch(It.IsAny<object>())).Returns(true);

            var filter = filterMock.Object;
            IEnumerable<KeyValuePair<object, DatedMessage<object>>> result = null!;
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

            var exceptedData = consumerData.Select(x =>

            new KeyValuePair<object, DatedMessage<object>>(x.Message.Key, new DatedMessage<object>(x.Message.Value, x.Message.Timestamp.UtcDateTime))
            ).ToList();
            exceptedData.RemoveAt(1);


            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(exceptedData);
        }
    }
}
