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
    private static IConsumer<object, byte[]> CreateSingleMessageConsumer(
        LoadingTopic topic,
        Partition partition,
        object key,
        string value,
        CancellationToken token)
    {
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartition>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(token)).Returns(new ConsumeResult<object, byte[]>
        {
            Message = new Message<object, byte[]>
            {
                Key = key,
                Value = Encoding.UTF8.GetBytes(value),
                Timestamp = Timestamp.Default
            },
            Offset = new Offset(0)
        });
        consumerMock.Setup(x => x.Close());
        consumerMock.Setup(x => x.Dispose());

        return consumerMock.Object;
    }

    private static SnapshotLoader<object, object> CreateLoader(
        ILogger<SnapshotLoader<object, object>> logger,
        IOptions<SnapshotLoaderConfiguration> config,
        Func<IConsumer<object, byte[]>> consumerFactory,
        ITopicWatermarkLoader topicWatermarkLoader,
        IMessageSorter<object, object> sorter,
        IMessageEncoder<byte[], object> encoder)
    {
        var readerLoggerMock = new Mock<ILogger<PartitionSnapshotReader<object, object>>>();
        readerLoggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(false);
        var compactorLoggerMock = new Mock<ILogger<SnapshotCompactor<object, object>>>();
        compactorLoggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(false);

        var partitionReader = new PartitionSnapshotReader<object, object>(
            readerLoggerMock.Object,
            config,
            consumerFactory,
            encoder);
        var batchReader = new PartitionSnapshotBatchReader<object, object>(
            config,
            partitionReader);
        var compactor = new SnapshotCompactor<object, object>(
            compactorLoggerMock.Object,
            sorter);

        return new SnapshotLoader<object, object>(
            logger,
            config,
            consumerFactory,
            topicWatermarkLoader,
            batchReader,
            compactor);
    }

    private static Mock<ILogger<SnapshotLoader<object, object>>> CreateLoggerMock()
    {
        var loggerMock = new Mock<ILogger<SnapshotLoader<object, object>>>();
        loggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(false);
        return loggerMock;
    }
}
