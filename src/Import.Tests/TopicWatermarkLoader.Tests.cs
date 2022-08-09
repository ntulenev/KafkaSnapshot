using Microsoft.Extensions.Options;

using Confluent.Kafka;

using FluentAssertions;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Sorting;

namespace KafkaAsTable.Tests
{
    public class TopicWatermarkLoaderTests
    {
        [Fact(DisplayName = "TopicWatermarkLoader can be created with valid params.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderCanBeCreated()
        {

            // Arrange
            var client = (new Mock<IAdminClient>()).Object;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());
            options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
            {

            });

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

            // Assert
            exception.Should().BeNull();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can't be created with null client.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderCantBeCreatedWithNullClient()
        {

            // Arrange
            var client = (IAdminClient)null!;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());
            options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
            {

            });

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can't be created with null options.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderCantBeCreatedWithNullOptions()
        {

            // Arrange
            var client = (new Mock<IAdminClient>()).Object;
            var options = (IOptions<TopicWatermarkLoaderConfiguration>)null!;

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(client, options));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can't be created with null options value.")]
        [Trait("Category", "Unit")]
        public void TopicWatermarkLoaderCantBeCreatedWithNullOptionsValue()
        {

            // Arrange
            var client = (new Mock<IAdminClient>()).Object;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());

            // Act
            var exception = Record.Exception(() => new TopicWatermarkLoader(client, options.Object));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can't load data with null factory.")]
        [Trait("Category", "Unit")]
        public async Task TopicWatermarkLoaderCantLoadWithNullFactory()
        {

            // Arrange
            var client = (new Mock<IAdminClient>()).Object;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());
            options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
            {

            });
            var loader = new TopicWatermarkLoader(client, options.Object);
            var consumerFactory = (Func<IConsumer<string, string>>)null!;
            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topicName = new LoadingTopic("test", true, new DateFilterRange(null!, null!),partitionFilter,sort);

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await loader.LoadWatermarksAsync(consumerFactory, topicName, CancellationToken.None).ConfigureAwait(false)
            );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can't load data with null topic.")]
        [Trait("Category", "Unit")]
        public async Task TopicWatermarkLoaderCantLoadWithNullTopic()
        {

            // Arrange
            var client = (new Mock<IAdminClient>()).Object;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());
            options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
            {

            });
            var loader = new TopicWatermarkLoader(client, options.Object);
            static IConsumer<string, string> consumerFactory() => null!;
            var topicName = (LoadingTopic)null!;

            // Act
            var exception = await Record.ExceptionAsync(async () =>
            await loader.LoadWatermarksAsync(consumerFactory, topicName, System.Threading.CancellationToken.None).ConfigureAwait(false)
            );

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "TopicWatermarkLoader can load watermarks with valid params.")]
        [Trait("Category", "Unit")]
        public async Task CanLoadWatermarksWithValidParams()
        {

            HashSet<int> partitionFilter = null!;
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topic = new LoadingTopic("test", true, new DateFilterRange(null!, null!),partitionFilter,sort);
            var clientMock = new Mock<IAdminClient>();
            var client = clientMock.Object;
            var timeout = 1;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());
            options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
            {
                AdminClientTimeout = TimeSpan.FromSeconds(timeout)
            });
            var loader = new TopicWatermarkLoader(client, options.Object);

            var consumerMock = new Mock<IConsumer<object, object>>();

            IConsumer<object, object> consumerFactory() => consumerMock!.Object;

            var adminClientPartition = new TopicPartition(topic.Value, new Partition(1));

            var adminParitions = new[] { adminClientPartition };

            var borkerMeta = new BrokerMetadata(1, "testHost", 1000);

            var partitionMeta = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);

            var topicMeta = new TopicMetadata(topic.Value, new[] { partitionMeta }.ToList(), null);

            var meta = new Confluent.Kafka.Metadata(
                    new[] { borkerMeta }.ToList(),
                    new[] { topicMeta }.ToList(), 1, "test"
                    );

            clientMock.Setup(c => c.GetMetadata(topic.Value, TimeSpan.FromSeconds(timeout))).Returns(meta);

            var offets = new WatermarkOffsets(new Offset(1), new Offset(2));

            consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition, TimeSpan.FromSeconds(timeout))).Returns(offets);

            TopicWatermark result = null!;

            // Act
            var exception = await Record.ExceptionAsync(async () => result = await loader.LoadWatermarksAsync(consumerFactory, topic, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();

            consumerMock.Verify(x => x.Close(), Times.Once);

            consumerMock.Verify(x => x.Dispose(), Times.Once);

            result.Should().NotBeNull();

            var watermarks = result.Watermarks.ToList();

            watermarks.Should().ContainSingle();

            clientMock.Verify(c => c.GetMetadata(topic.Value, TimeSpan.FromSeconds(timeout)), Times.Once);

            consumerMock.Verify(x => x.QueryWatermarkOffsets(adminClientPartition, TimeSpan.FromSeconds(timeout)), Times.Once);

            watermarks.Single().TopicName.Should().Be(topic);

            watermarks.Single().Partition.Value.Should().Be(partitionMeta.PartitionId);

            watermarks.Single().Offset.Should().Be(offets);
        }

        [Fact(DisplayName = "TopicWatermarkLoader can load watermarks with valid params with partition filter.")]
        [Trait("Category", "Unit")]
        public async Task CanLoadWatermarksWithValidParamsWithPartitionFilter()
        {
            var sort = new SortingParams(SortingType.Time, SortingOrder.No);
            var topic = new LoadingTopic("test", true, new DateFilterRange(null!, null!), new HashSet<int>(new[] { 2 }),sort);
            var clientMock = new Mock<IAdminClient>();
            var client = clientMock.Object;
            var timeout = 1;
            var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>());
            options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
            {
                AdminClientTimeout = TimeSpan.FromSeconds(timeout)
            });
            var loader = new TopicWatermarkLoader(client, options.Object);

            var consumerMock = new Mock<IConsumer<object, object>>();

            IConsumer<object, object> consumerFactory() => consumerMock!.Object;

            var adminClientPartition1 = new TopicPartition(topic.Value, new Partition(1));
            var adminClientPartition2 = new TopicPartition(topic.Value, new Partition(2));

            var adminParitions = new[] { adminClientPartition1, adminClientPartition2 };

            var borkerMeta = new BrokerMetadata(1, "testHost", 1000);

            var partitionMeta1 = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);
            var partitionMeta2 = new PartitionMetadata(2, 1, new[] { 1 }, new[] { 1 }, null);

            var topicMeta = new TopicMetadata(topic.Value, new[] { partitionMeta1, partitionMeta2 }.ToList(), null);

            var meta = new Confluent.Kafka.Metadata(
                    new[] { borkerMeta }.ToList(),
                    new[] { topicMeta }.ToList(), 1, "test"
                    );

            clientMock.Setup(c => c.GetMetadata(topic.Value, TimeSpan.FromSeconds(timeout))).Returns(meta);

            var offets = new WatermarkOffsets(new Offset(1), new Offset(2));

            consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition1, TimeSpan.FromSeconds(timeout))).Returns(offets);
            consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition2, TimeSpan.FromSeconds(timeout))).Returns(offets);

            TopicWatermark result = null!;

            // Act
            var exception = await Record.ExceptionAsync(async () => result = await loader.LoadWatermarksAsync(consumerFactory, topic, CancellationToken.None).ConfigureAwait(false));

            // Assert
            exception.Should().BeNull();

            consumerMock.Verify(x => x.Close(), Times.Once);

            consumerMock.Verify(x => x.Dispose(), Times.Once);

            result.Should().NotBeNull();

            var watermarks = result.Watermarks.ToList();

            watermarks.Should().ContainSingle();

            clientMock.Verify(c => c.GetMetadata(topic.Value, TimeSpan.FromSeconds(timeout)), Times.Once);

            consumerMock.Verify(x => x.QueryWatermarkOffsets(adminClientPartition1, TimeSpan.FromSeconds(timeout)), Times.Never);
            consumerMock.Verify(x => x.QueryWatermarkOffsets(adminClientPartition2, TimeSpan.FromSeconds(timeout)), Times.Once);

            watermarks.Single().TopicName.Should().Be(topic);

            watermarks.Single().Partition.Value.Should().Be(partitionMeta2.PartitionId);

            watermarks.Single().Offset.Should().Be(offets);
        }
    }
}
