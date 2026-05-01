using System.Text;

using Confluent.Kafka;

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

namespace KafkaSnapshot.Import.Tests;

public partial class PartitionSnapshotReaderTests
{
    private static Mock<ILogger<PartitionSnapshotReader<object, object>>> CreateLoggerMock()
    {
        var loggerMock = new Mock<ILogger<PartitionSnapshotReader<object, object>>>();
        loggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>()))
            .Returns(false);

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
