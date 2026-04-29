using Microsoft.Extensions.Options;

using Confluent.Kafka;

using FluentAssertions;

using Moq;

using Xunit;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Names;

namespace KafkaAsTable.Tests;

public class TopicWatermarkLoaderTests
{
    [Fact(DisplayName = "TopicWatermarkLoader can be created with valid params.")]
    [Trait("Category", "Unit")]
    public void TopicWatermarkLoaderCanBeCreated()
    {

        // Arrange
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {

        });

        // Act
        var exception = Record.Exception(() => CreateLoader(client, options.Object));

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't be created with null client.")]
    [Trait("Category", "Unit")]
    public void TopicWatermarkLoaderCantBeCreatedWithNullClient()
    {

        // Arrange
        var client = (IKafkaClientFactory)null!;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
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
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (IOptions<TopicWatermarkLoaderConfiguration>)null!;

        // Act
        var exception = Record.Exception(() => new TopicWatermarkLoader(
            CreateKafkaClientFactory(client),
            options));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't be created with null options value.")]
    [Trait("Category", "Unit")]
    public void TopicWatermarkLoaderCantBeCreatedWithNullOptionsValue()
    {

        // Arrange
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.SetupGet(x => x.Value).Returns(() => null!);

        // Act
        var exception = Record.Exception(() => CreateLoader(client, options.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't load data with null factory.")]
    [Trait("Category", "Unit")]
    public async Task TopicWatermarkLoaderCantLoadWithNullFactory()
    {

        // Arrange
        using var cts = new CancellationTokenSource();
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {

        });
        var loader = CreateLoader(client, options.Object);
        var consumerFactory = (Func<IConsumer<string, string>>)null!;
        HashSet<int> partitionFilter = null!;
        var topicName = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        await loader.LoadWatermarksAsync(consumerFactory, topicName, cts.Token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can't load data with null topic.")]
    [Trait("Category", "Unit")]
    public async Task TopicWatermarkLoaderCantLoadWithNullTopic()
    {

        // Arrange
        using var cts = new CancellationTokenSource();
        var client = (new Mock<IAdminClient>(MockBehavior.Strict)).Object;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {

        });
        var loader = CreateLoader(client, options.Object);
        static IConsumer<string, string> consumerFactory() => null!;
        var topicName = (LoadingTopic)null!;

        // Act
        var exception = await Record.ExceptionAsync(async () =>
        await loader.LoadWatermarksAsync(consumerFactory, topicName, cts.Token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "TopicWatermarkLoader can load watermarks with valid params.")]
    [Trait("Category", "Unit")]
    public async Task CanLoadWatermarksWithValidParams()
    {

        //Arrange
        using var cts = new CancellationTokenSource();
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var clientMock = new Mock<IAdminClient>(MockBehavior.Strict);
        var client = clientMock.Object;
        var timeout = 1;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {
            AdminClientTimeout = TimeSpan.FromSeconds(timeout)
        });
        var loader = CreateLoader(client, options.Object);
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        IConsumer<object, object> consumerFactory() => consumerMock!.Object;
        var adminClientPartition = new TopicPartition(topic.Value.Name, new Partition(1));
        var adminParitions = new[] { adminClientPartition };
        var borkerMeta = new BrokerMetadata(1, "testHost", 1000);
        var partitionMeta = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);
        var topicMeta = new TopicMetadata(topic.Value.Name, new[] { partitionMeta }.ToList(), null);
        var meta = new Confluent.Kafka.Metadata(
                new[] { borkerMeta }.ToList(),
                new[] { topicMeta }.ToList(), 1, "test"
                );
        var getMetadataCalls = 0;
        clientMock.Setup(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout)))
            .Callback(() => getMetadataCalls++)
            .Returns(meta);
        var offets = new WatermarkOffsets(new Offset(1), new Offset(2));
        var queryWatermarkOffsetsCalls = 0;
        consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition, TimeSpan.FromSeconds(timeout)))
            .Callback(() => queryWatermarkOffsetsCalls++)
            .Returns(offets);
        var disposeCalls = 0;
        consumerMock.Setup(x => x.Dispose())
            .Callback(() => disposeCalls++);
        var closeCalls = 0;
        consumerMock.Setup(x => x.Close())
            .Callback(() => closeCalls++);

        // Act
        var result = await loader.LoadWatermarksAsync(consumerFactory, topic, cts.Token);

        // Assert
        result.Should().NotBeNull();
        var watermarks = result.Watermarks.ToList();
        watermarks.Should().ContainSingle();
        watermarks.Single().TopicName.Should().Be(topic);
        watermarks.Single().Partition.Value.Should().Be(partitionMeta.PartitionId);
        watermarks.Single().Offset.Should().Be(offets);
        getMetadataCalls.Should().Be(1);
        queryWatermarkOffsetsCalls.Should().Be(1);
        closeCalls.Should().Be(1);
        disposeCalls.Should().Be(1);
    }

    [Fact(DisplayName = "TopicWatermarkLoader can load watermarks with valid params with partition filter.")]
    [Trait("Category", "Unit")]
    public async Task CanLoadWatermarksWithValidParamsWithPartitionFilter()
    {
        //Arrange
        using var cts = new CancellationTokenSource();
        var topic = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), EncoderRules.String, new HashSet<int>(new[] { 2 }));
        var clientMock = new Mock<IAdminClient>(MockBehavior.Strict);
        var client = clientMock.Object;
        var timeout = 1;
        var options = (new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict));
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {
            AdminClientTimeout = TimeSpan.FromSeconds(timeout)
        });
        var loader = CreateLoader(client, options.Object);
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        IConsumer<object, object> consumerFactory() => consumerMock!.Object;
        var adminClientPartition1 = new TopicPartition(topic.Value.Name, new Partition(1));
        var adminClientPartition2 = new TopicPartition(topic.Value.Name, new Partition(2));
        var adminParitions = new[] { adminClientPartition1, adminClientPartition2 };
        var borkerMeta = new BrokerMetadata(1, "testHost", 1000);
        var partitionMeta1 = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);
        var partitionMeta2 = new PartitionMetadata(2, 1, new[] { 1 }, new[] { 1 }, null);
        var topicMeta = new TopicMetadata(topic.Value.Name, new[] { partitionMeta1, partitionMeta2 }.ToList(), null);
        var meta = new Confluent.Kafka.Metadata(
                new[] { borkerMeta }.ToList(),
                new[] { topicMeta }.ToList(), 1, "test"
                );
        var getMetadataCalls = 0;
        clientMock.Setup(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout)))
            .Callback(() => getMetadataCalls++)
            .Returns(meta);
        var offets = new WatermarkOffsets(new Offset(1), new Offset(2));
        var queryWatermarkOffsetsPartition1Calls = 0;
        consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition1, TimeSpan.FromSeconds(timeout)))
            .Callback(() => queryWatermarkOffsetsPartition1Calls++)
            .Returns(offets);
        var queryWatermarkOffsetsPartition2Calls = 0;
        consumerMock.Setup(x => x.QueryWatermarkOffsets(adminClientPartition2, TimeSpan.FromSeconds(timeout)))
            .Callback(() => queryWatermarkOffsetsPartition2Calls++)
            .Returns(offets);
        var disposeCalls = 0;
        consumerMock.Setup(x => x.Dispose())
            .Callback(() => disposeCalls++);
        var closeCalls = 0;
        consumerMock.Setup(x => x.Close())
            .Callback(() => closeCalls++);

        // Act
        var result = await loader.LoadWatermarksAsync(consumerFactory, topic, cts.Token);

        // Assert
        result.Should().NotBeNull();
        var watermarks = result.Watermarks.ToList();
        watermarks.Should().ContainSingle();
        watermarks.Single().TopicName.Should().Be(topic);
        watermarks.Single().Partition.Value.Should().Be(partitionMeta2.PartitionId);
        watermarks.Single().Offset.Should().Be(offets);
        getMetadataCalls.Should().Be(1);
        queryWatermarkOffsetsPartition1Calls.Should().Be(0);
        queryWatermarkOffsetsPartition2Calls.Should().Be(1);
        closeCalls.Should().Be(1);
        disposeCalls.Should().Be(1);
    }

    [Fact(DisplayName = "TopicWatermarkLoader fails when metadata for requested topic is missing.")]
    [Trait("Category", "Unit")]
    public async Task TopicWatermarkLoaderFailsWhenMetadataForRequestedTopicIsMissing()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var clientMock = new Mock<IAdminClient>(MockBehavior.Strict);
        var timeout = 1;
        var options = new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict);
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {
            AdminClientTimeout = TimeSpan.FromSeconds(timeout)
        });
        var loader = CreateLoader(clientMock.Object, options.Object);
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        IConsumer<object, object> consumerFactory() => consumerMock.Object;

        var brokerMeta = new BrokerMetadata(1, "testHost", 1000);
        var partitionMeta = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);
        var anotherTopicMeta = new TopicMetadata("another-topic", new[] { partitionMeta }.ToList(), null);
        var metadata = new Confluent.Kafka.Metadata(
            new[] { brokerMeta }.ToList(),
            new[] { anotherTopicMeta }.ToList(),
            1,
            "test");
        var getMetadataCalls = 0;
        clientMock.Setup(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout)))
                  .Callback(() => getMetadataCalls++)
                  .Returns(metadata);
        var closeCalls = 0;
        consumerMock.Setup(x => x.Close())
                    .Callback(() => closeCalls++);
        var disposeCalls = 0;
        consumerMock.Setup(x => x.Dispose())
                    .Callback(() => disposeCalls++);

        // Act
        var exception = await Record.ExceptionAsync(
            async () => await loader.LoadWatermarksAsync(consumerFactory, topic, cts.Token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain(topic.Value.Name).And.Contain("not found");
        getMetadataCalls.Should().Be(1);
        closeCalls.Should().Be(1);
        disposeCalls.Should().Be(1);
    }

    [Fact(DisplayName = "TopicWatermarkLoader fails when requested topic metadata has broker error.")]
    [Trait("Category", "Unit")]
    public async Task TopicWatermarkLoaderFailsWhenRequestedTopicMetadataHasBrokerError()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var clientMock = new Mock<IAdminClient>(MockBehavior.Strict);
        var timeout = 1;
        var options = new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict);
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {
            AdminClientTimeout = TimeSpan.FromSeconds(timeout)
        });
        var loader = CreateLoader(clientMock.Object, options.Object);
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        IConsumer<object, object> consumerFactory() => consumerMock.Object;

        var brokerMeta = new BrokerMetadata(1, "testHost", 1000);
        var topicMeta = new TopicMetadata(
            topic.Value.Name,
            [],
            new Error(ErrorCode.UnknownTopicOrPart, "Unknown topic"));
        var metadata = new Confluent.Kafka.Metadata(
            new[] { brokerMeta }.ToList(),
            new[] { topicMeta }.ToList(),
            1,
            "test");
        var getMetadataCalls = 0;
        clientMock.Setup(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout)))
                  .Callback(() => getMetadataCalls++)
                  .Returns(metadata);
        var closeCalls = 0;
        consumerMock.Setup(x => x.Close())
                    .Callback(() => closeCalls++);
        var disposeCalls = 0;
        consumerMock.Setup(x => x.Dispose())
                    .Callback(() => disposeCalls++);

        // Act
        var exception = await Record.ExceptionAsync(
            async () => await loader.LoadWatermarksAsync(consumerFactory, topic, cts.Token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain(topic.Value.Name).And.Contain(nameof(ErrorCode.UnknownTopicOrPart));
        getMetadataCalls.Should().Be(1);
        closeCalls.Should().Be(1);
        disposeCalls.Should().Be(1);
    }

    [Fact(DisplayName = "TopicWatermarkLoader can load watermarks when metadata contains additional topics.")]
    [Trait("Category", "Unit")]
    public async Task CanLoadWatermarksWhenMetadataContainsAdditionalTopics()
    {
        // Arrange
        using var cts = new CancellationTokenSource();
        HashSet<int> partitionFilter = null!;
        var topic = new LoadingTopic(new TopicName("test"), true, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var clientMock = new Mock<IAdminClient>(MockBehavior.Strict);
        var timeout = 1;
        var options = new Mock<IOptions<TopicWatermarkLoaderConfiguration>>(MockBehavior.Strict);
        options.Setup(x => x.Value).Returns(new TopicWatermarkLoaderConfiguration
        {
            AdminClientTimeout = TimeSpan.FromSeconds(timeout)
        });
        var loader = CreateLoader(clientMock.Object, options.Object);
        var consumerMock = new Mock<IConsumer<object, object>>(MockBehavior.Strict);
        IConsumer<object, object> consumerFactory() => consumerMock.Object;

        var requestedPartition = new TopicPartition(topic.Value.Name, new Partition(1));
        var offsets = new WatermarkOffsets(new Offset(1), new Offset(2));

        var brokerMeta = new BrokerMetadata(1, "testHost", 1000);
        var requestedPartitionMeta = new PartitionMetadata(1, 1, new[] { 1 }, new[] { 1 }, null);
        var anotherTopicPartitionMeta = new PartitionMetadata(2, 1, new[] { 1 }, new[] { 1 }, null);
        var anotherTopicMeta = new TopicMetadata("another-topic", new[] { anotherTopicPartitionMeta }.ToList(), null);
        var requestedTopicMeta = new TopicMetadata(topic.Value.Name, new[] { requestedPartitionMeta }.ToList(), null);
        var metadata = new Confluent.Kafka.Metadata(
            new[] { brokerMeta }.ToList(),
            new[] { anotherTopicMeta, requestedTopicMeta }.ToList(),
            1,
            "test");
        var getMetadataCalls = 0;
        clientMock.Setup(c => c.GetMetadata(topic.Value.Name, TimeSpan.FromSeconds(timeout)))
                  .Callback(() => getMetadataCalls++)
                  .Returns(metadata);
        var queryWatermarkOffsetsCalls = 0;
        consumerMock.Setup(x => x.QueryWatermarkOffsets(requestedPartition, TimeSpan.FromSeconds(timeout)))
                    .Callback(() => queryWatermarkOffsetsCalls++)
                    .Returns(offsets);
        var closeCalls = 0;
        consumerMock.Setup(x => x.Close())
                    .Callback(() => closeCalls++);
        var disposeCalls = 0;
        consumerMock.Setup(x => x.Dispose())
                    .Callback(() => disposeCalls++);

        // Act
        var result = await loader.LoadWatermarksAsync(consumerFactory, topic, cts.Token);

        // Assert
        result.Should().NotBeNull();
        var watermarks = result.Watermarks.ToList();
        watermarks.Should().ContainSingle();
        watermarks.Single().TopicName.Should().Be(topic);
        watermarks.Single().Partition.Value.Should().Be(requestedPartitionMeta.PartitionId);
        watermarks.Single().Offset.Should().Be(offsets);
        getMetadataCalls.Should().Be(1);
        queryWatermarkOffsetsCalls.Should().Be(1);
        closeCalls.Should().Be(1);
        disposeCalls.Should().Be(1);
    }

    private static TopicWatermarkLoader CreateLoader(
        IAdminClient adminClient,
        IOptions<TopicWatermarkLoaderConfiguration> options)
        => new(CreateKafkaClientFactory(adminClient), options);

    private static IKafkaClientFactory CreateKafkaClientFactory(IAdminClient adminClient)
    {
        if (adminClient is not null)
        {
            Mock.Get(adminClient).Setup(client => client.Dispose());
        }

        var factoryMock = new Mock<IKafkaClientFactory>(MockBehavior.Strict);
        factoryMock.Setup(factory => factory.CreateAdminClient()).Returns(adminClient!);

        return factoryMock.Object;
    }
}


