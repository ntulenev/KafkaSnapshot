using System.Text;

using Confluent.Kafka;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Moq;

namespace KafkaSnapshot.Import.Tests;

public partial class SnapshotLoaderTests
{
    private static IConsumer<object, byte[]> CreateSingleMessageConsumer(
        LoadingTopic topic,
        Partition partition,
        object key,
        string value)
    {
        var consumerMock = new Mock<IConsumer<object, byte[]>>(MockBehavior.Strict);
        consumerMock.Setup(x => x.Assign(It.Is<TopicPartitionOffset>(
            item => item.Topic == topic.Value.Name && item.Partition == partition)));
        consumerMock.Setup(x => x.Consume(It.Is<CancellationToken>(current => current.CanBeCanceled))).Returns(new ConsumeResult<object, byte[]>
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
        var compactorLoggerMock = new Mock<ILogger<SnapshotBuilder<object, object>>>();
        compactorLoggerMock.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(false);

        var partitionReader = new PartitionSnapshotReader<object, object>(
            readerLoggerMock.Object,
            config,
            consumerFactory,
            encoder, new KafkaSnapshot.Import.Kafka.PartitionConsumerAssignment());
        var batchReader = new PartitionSnapshotBatchReader<object, object>(
            config,
            partitionReader);
        var compactor = new SnapshotBuilder<object, object>(
            compactorLoggerMock.Object,
            sorter);

        return new SnapshotLoader<object, object>(
            logger,
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
