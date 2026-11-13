using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Import.Configuration;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public partial class PartitionSnapshotReaderTests
{
    [Fact(DisplayName = "PartitionSnapshotReader reports a null consumer returned by its factory without masking the argument error.")]
    [Trait("Category", "Unit")]
    public void NullConsumerFromFactoryPreservesArgumentError()
    {
        var topic = CreateTopic();
        var watermark = CreateWatermark(topic, CreatePartition());
        var (keyFilter, valueFilter) = CreateStrictFilters();
        var reader = new PartitionSnapshotReader<object, object>(CreateLoggerMock().Object,
            Options.Create(new SnapshotLoaderConfiguration()), () => null!,
            new Mock<IMessageEncoder<byte[], object>>(MockBehavior.Strict).Object, new KafkaSnapshot.Import.Kafka.PartitionConsumerAssignment());

        Action act = () => _ = reader.Read(watermark, topic, keyFilter.Object, valueFilter.Object, CancellationToken.None).ToList();

        act.Should().Throw<ArgumentNullException>().WithParameterName("consumer");
    }

    [Theory(DisplayName = "PartitionSnapshotReader skips rejected keys or values and releases the consumer.")]
    [Trait("Category", "Unit")]
    [InlineData(false, true)]
    [InlineData(true, false)]
    public void RejectedMessagesAreExcluded(bool keyMatches, bool valueMatches)
    {
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumer = CreateReadableConsumerMock(topic, partition, new Offset(0), CancellationToken.None);
        var encoder = CreateEncoderMock();
        var (keyFilter, valueFilter) = CreateMatchingFilters();
        keyFilter.Setup(x => x.IsMatch("key")).Returns(keyMatches);
        valueFilter.Setup(x => x.IsMatch("value")).Returns(valueMatches);
        var reader = CreateReader(CreateLoggerMock().Object, consumer, encoder);

        reader.Read(watermark, topic, keyFilter.Object, valueFilter.Object, CancellationToken.None).Should().BeEmpty();

        valueFilter.Verify(x => x.IsMatch("value"), keyMatches ? Times.Once() : Times.Never());
        consumer.Verify(x => x.Close(), Times.Once);
        consumer.Verify(x => x.Dispose(), Times.Once);
    }

    [Fact(DisplayName = "PartitionSnapshotReader traces decoded messages and preserves their key, value, and offset.")]
    [Trait("Category", "Unit")]
    public void TraceLoggingPreservesDecodedMessage()
    {
        var logger = new RecordingLogger<PartitionSnapshotReader<object, object>>(LogLevel.Trace);
        var topic = CreateTopic();
        var partition = CreatePartition();
        var watermark = CreateWatermark(topic, partition);
        var consumer = CreateReadableConsumerMock(topic, partition, new Offset(0), CancellationToken.None);
        var encoder = CreateEncoderMock();
        var (keyFilter, valueFilter) = CreateMatchingFilters();
        var reader = CreateReader(logger, consumer, encoder);

        var result = reader.Read(watermark, topic, keyFilter.Object, valueFilter.Object, CancellationToken.None).ToList();

        result.Should().ContainSingle().Which.Key.Should().Be("key");
        result[0].Value.Message.Should().Be("value");
        result[0].Value.Meta.Offset.Should().Be(0);
        logger.Contains(LogLevel.Trace, "Loading key - value").Should().BeTrue();
        consumer.Verify(x => x.Close(), Times.Once);
        consumer.Verify(x => x.Dispose(), Times.Once);
    }
}
