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
    [Fact(DisplayName = "SnapshotLoader can't load for null topic.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCantLoadForNullTopic()
    {

        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
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
        var loader = CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var filterKeyMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var keyFilter = filterKeyMock.Object;
        var filterValueMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilter = filterValueMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadSnapshotAsync(
                null!, keyFilter, valueFilter, token));


        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "SnapshotLoader can't load for null key filter.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCantLoadForNullKeyFilter()
    {

        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
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
        var loader = CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicParams = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var filterValueMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var valueFilter = filterValueMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadSnapshotAsync(
                topicParams, null!, valueFilter, token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }


    [Fact(DisplayName = "SnapshotLoader can't load for null value filter.")]
    [Trait("Category", "Unit")]
    public async Task SnapshotLoaderCantLoadForNullValueFilter()
    {

        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
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
        var loader = CreateLoader(logger, options, consumerFactory, topicLoader, sorter, encoder);
        var withCompacting = true;
        HashSet<int> partitionFilter = null!;
        var topicParams = new LoadingTopic(new TopicName("test"), withCompacting, new DateFilterRange(null!, null!), EncoderRules.String, partitionFilter);
        var filterKeyMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        var keyFilter = filterKeyMock.Object;

        // Act
        var exception = await Record.ExceptionAsync(
            async () => _ = await loader.LoadSnapshotAsync(
                topicParams, keyFilter, null!, token));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }
}
