using System.Text;

using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class PartitionSnapshotReaderTests
{
    [Fact(DisplayName = "PartitionSnapshotReader can't be created with null logger.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderCantBeCreatedWithNullLogger()
    {
        // Arrange
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        IConsumer<object, byte[]> consumerFactory() => Mock.Of<IConsumer<object, byte[]>>();

        // Act
        var exception = Record.Exception(() =>
            new PartitionSnapshotReader<object, object>(
                null!,
                Options.Create(new SnapshotLoaderConfiguration()),
                consumerFactory,
                encoderMock.Object));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "PartitionSnapshotReader reads matching partition messages.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderReadsMatchingPartitionMessages()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(
            LogLevel.Information,
            LogLevel.Warning);
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = CreateReadableConsumerMock(topic, partition, new Offset(0), token);
        var encoderMock = CreateEncoderMock();
        var (keyFilterMock, valueFilterMock) = CreateMatchingFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().ContainSingle();
        result[0].Key.Should().Be("key");
        result[0].Value.Message.Should().Be("value");
        result[0].Value.Meta.Partition.Should().Be(partition.Value);
    }

    [Fact(DisplayName = "PartitionSnapshotReader skips partition without date offset.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderSkipsPartitionWithoutDateOffset()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(
            LogLevel.Information,
            LogLevel.Warning);
        var startDate = DateTime.UtcNow;
        var topic = CreateTopic(new DateFilterRange(startDate, null!));
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        var topicPartition = new TopicPartition(topic.Value.Name, partition);
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(items =>
                items.Single().TopicPartition == topicPartition),
            It.IsAny<TimeSpan>())).Returns(
            [
                new TopicPartitionOffset(topicPartition, Offset.End)
            ]);
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().BeEmpty();
        logger.Contains(LogLevel.Information, "Searching offset for topic test, partition 1").Should().BeTrue();
        logger.Contains(LogLevel.Warning, "No actual offset for topic test, partition 1").Should().BeTrue();
    }

    [Fact(DisplayName = "PartitionSnapshotReader logs resolved date offset.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderLogsResolvedDateOffset()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(LogLevel.Information);
        var startDate = DateTimeOffset.UtcNow;
        var topic = CreateTopic(new DateFilterRange(startDate, null!));
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        var topicPartition = new TopicPartition(topic.Value.Name, partition);
        consumerMock.Setup(x => x.OffsetsForTimes(
            It.Is<IEnumerable<TopicPartitionTimestamp>>(items =>
                items.Single().TopicPartition == topicPartition),
            It.IsAny<TimeSpan>())).Returns(
            [
                new TopicPartitionOffset(topicPartition, new Offset(0))
            ]);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartitionOffset>(
            item => item.TopicPartition == topicPartition && item.Offset == new Offset(0))));
        consumerMock.Setup(x => x.Consume(token)).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Key = "key",
                Value = Encoding.UTF8.GetBytes("value"),
                Timestamp = Timestamp.Default
            },
            Offset = new Offset(0)
        });
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = CreateEncoderMock();
        var (keyFilterMock, valueFilterMock) = CreateMatchingFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().ContainSingle();
        logger.Contains(LogLevel.Information, "Searching offset for topic test, partition 1").Should().BeTrue();
        logger.Contains(LogLevel.Information, "Resolved offset 0 for topic test, partition 1").Should().BeTrue();
    }

    [Fact(DisplayName = "PartitionSnapshotReader logs final date offset reached.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderLogsFinalDateOffsetReached()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(LogLevel.Information);
        var endDate = new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var topic = CreateTopic(new DateFilterRange(null!, endDate));
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Key = "key",
                Value = Encoding.UTF8.GetBytes("value"),
                Timestamp = new Timestamp(endDate.AddSeconds(1).UtcDateTime)
            },
            Offset = new Offset(0)
        });
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(logger, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().BeEmpty();
        logger.Contains(LogLevel.Information, "Final date offset").Should().BeTrue();
    }

    [Fact(DisplayName = "PartitionSnapshotReader closes consumer when enumeration completes.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderClosesConsumerWhenEnumerationCompletes()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = CreateReadableConsumerMock(topic, partition, new Offset(0), token);
        var encoderMock = CreateEncoderMock();
        var (keyFilterMock, valueFilterMock) = CreateMatchingFilters();
        var reader = CreateReader(loggerMock.Object, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().ContainSingle();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader closes consumer when final date stops enumeration.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderClosesConsumerWhenFinalDateStopsEnumeration()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var endDate = new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var topic = CreateTopic(new DateFilterRange(null!, endDate));
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Key = "key",
                Value = Encoding.UTF8.GetBytes("value"),
                Timestamp = new Timestamp(endDate.AddSeconds(1).UtcDateTime)
            },
            Offset = new Offset(0)
        });
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(loggerMock.Object, consumerMock, encoderMock);

        // Act
        var result = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList();

        // Assert
        result.Should().BeEmpty();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader closes consumer when consume throws.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderClosesConsumerWhenConsumeThrows()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Throws(new OperationCanceledException(token));
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        var (keyFilterMock, valueFilterMock) = CreateStrictFilters();
        var reader = CreateReader(loggerMock.Object, consumerMock, encoderMock);

        // Act
        var exception = Record.Exception(() => reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).ToList());

        // Assert
        exception.Should().BeOfType<OperationCanceledException>();
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader closes consumer when caller stops enumeration early.")]
    [Trait("Category", "Unit")]
    public void PartitionSnapshotReaderClosesConsumerWhenCallerStopsEnumerationEarly()
    {
        // Arrange
        using var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;
        var loggerMock = CreateLoggerMock();
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition, highOffset: 2);
        var consumerMock = CreateReadableConsumerMock(topic, partition, new Offset(0), token);
        var encoderMock = CreateEncoderMock();
        var (keyFilterMock, valueFilterMock) = CreateMatchingFilters();
        var reader = CreateReader(loggerMock.Object, consumerMock, encoderMock);

        // Act
        using (var enumerator = reader.Read(
            watermark,
            topic,
            keyFilterMock.Object,
            valueFilterMock.Object,
            token).GetEnumerator())
        {
            enumerator.MoveNext().Should().BeTrue();
        }

        // Assert
        consumerMock.Verify(x => x.Close(), Times.Once);
        consumerMock.Verify(x => x.Dispose(), Times.Once);
    }

    private static Mock<ILogger<PartitionSnapshotReader<object, object>>> CreateLoggerMock(
        params LogLevel[] enabledLogLevels)
    {
        var loggerMock = new Mock<ILogger<PartitionSnapshotReader<object, object>>>(MockBehavior.Strict);
        var enabled = enabledLogLevels.ToHashSet();
        loggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>()))
            .Returns((LogLevel level) => enabled.Contains(level));
        loggerMock.Setup(x => x.BeginScope(It.IsAny<It.IsAnyType>()))
            .Returns(Mock.Of<IDisposable>());
        loggerMock.Setup(
            x => x.Log(
                It.IsAny<LogLevel>(),
                It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()));

        return loggerMock;
    }

    private static LoadingTopic CreateTopic(DateFilterRange? dateRange = null)
        => new(
            new TopicName("test"),
            false,
            dateRange ?? new DateFilterRange(null!, null!),
            EncoderRules.String,
            null);

    private static Partition CreatePartition() => new(1);

    private static PartitionWatermark CreateWatermark(
        LoadingTopic topic,
        Partition partition,
        long highOffset = 1)
        => new(
            topic,
            new WatermarkOffsets(new Offset(0), new Offset(highOffset)),
            partition);

    private static PartitionSnapshotReader<object, object> CreateReader(
        ILogger<PartitionSnapshotReader<object, object>> logger,
        Mock<IConsumer<object, byte[]>> consumerMock,
        Mock<IMessageEncoder<byte[], object>> encoderMock)
        => new(
            logger,
            Options.Create(new SnapshotLoaderConfiguration()),
            () => consumerMock.Object,
            encoderMock.Object);

    private static Mock<IMessageEncoder<byte[], object>> CreateEncoderMock()
    {
        var encoderMock = new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict);
        encoderMock.Setup(x => x.Encode(It.IsAny<byte[]>(), EncoderRules.String)).Returns("value");

        return encoderMock;
    }

    private static (
        Mock<IDataFilter<object>> KeyFilter,
        Mock<IDataFilter<object>> ValueFilter) CreateMatchingFilters()
    {
        var keyFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        keyFilterMock.Setup(x => x.IsMatch("key")).Returns(true);
        var valueFilterMock = new Mock<IDataFilter<object>>(MockBehavior.Strict);
        valueFilterMock.Setup(x => x.IsMatch("value")).Returns(true);

        return (keyFilterMock, valueFilterMock);
    }

    private static (
        Mock<IDataFilter<object>> KeyFilter,
        Mock<IDataFilter<object>> ValueFilter) CreateStrictFilters()
        => (
            new Mock<IDataFilter<object>>(MockBehavior.Strict),
            new Mock<IDataFilter<object>>(MockBehavior.Strict));

    private static Mock<IConsumer<object, byte[]>> CreateReadableConsumerMock(
        LoadingTopic topic,
        Partition partition,
        Offset offset,
        CancellationToken token)
    {
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Key = "key",
                Value = Encoding.UTF8.GetBytes("value"),
                Timestamp = Timestamp.Default
            },
            Offset = offset
        });
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());

        return consumerMock;
    }

    private sealed class RecordingLogger<T>(params LogLevel[] enabledLogLevels) : ILogger<T>
    {
        private readonly HashSet<LogLevel> _enabledLogLevels = enabledLogLevels.ToHashSet();
        private readonly List<(LogLevel Level, string Message)> _entries = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => _enabledLogLevels.Contains(logLevel);

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            _entries.Add((logLevel, formatter(state, exception)));
        }

        public bool Contains(LogLevel logLevel, string message)
            => _entries.Any(entry =>
                entry.Level == logLevel &&
                entry.Message.Contains(message, StringComparison.Ordinal));
    }

    private sealed class NullScope : IDisposable
    {
        public static NullScope Instance { get; } = new();

        public void Dispose()
        {
        }
    }
}
