using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;

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
        public async Task SnapshotLoaderCantLoadForNullTopicAsync()
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
                async () => _ = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None));


            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SnapshotLoader can't load for null filter.")]
        [Trait("Category", "Unit")]
        public async Task SnapshotLoaderCantLoadForNullFilterAsync()
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
            bool withCompacting = true;
            var topicParams = new LoadingTopic("test", withCompacting);
            var filter = (IKeyFilter<object>)null!;
            // Act
            var exception = await Record.ExceptionAsync(
                async () => _ = await loader.LoadCompactSnapshotAsync(topicParams, filter, CancellationToken.None));


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
            bool withCompacting = false;
            var topicName = new LoadingTopic("test", withCompacting);
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

            int indexer = 0;
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

            IEnumerable<KeyValuePair<object, DatedMessage<object>>> exceptedData = consumerData.Select(x =>

            new KeyValuePair<object, DatedMessage<object>>(x.Message.Key, new DatedMessage<object>(x.Message.Value, x.Message.Timestamp.UtcDateTime)));

            consumerMock.Setup(x => x.Consume(CancellationToken.None)).Returns(() =>
            {
                return consumerData[indexer++];
            });

            // Act
            var exception = await Record.ExceptionAsync(
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(exceptedData);
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
            bool withCompacting = true;
            var topicName = new LoadingTopic("test", withCompacting);
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

            int indexer = 0;
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
                async () => result = await loader.LoadCompactSnapshotAsync(topicName, filter, CancellationToken.None));

            // Assert
            exception.Should().BeNull();
            result.Should().BeEquivalentTo(exceptedData);
        }
    }
}
