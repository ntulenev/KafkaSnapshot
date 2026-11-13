using Confluent.Kafka;

using FluentAssertions;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;

using Microsoft.Extensions.Options;

using Moq;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

[Trait("Category", "Unit")]
public class TopicWatermarkLoaderFailureTests
{
    [Theory(DisplayName = "TopicWatermarkLoader rejects ambiguous metadata or missing partitions and releases clients.")]
    [InlineData(true, "ambiguous")]
    [InlineData(false, "does not contain partitions")]
    public async Task InvalidMetadataReleasesAdminAndConsumer(bool ambiguous, string error)
    {
        var topicMeta = new TopicMetadata("customers", ambiguous ? [] : null!, new Error(ErrorCode.NoError));
        var metadata = new Confluent.Kafka.Metadata([], ambiguous ? [topicMeta, topicMeta] : [topicMeta], 0, "broker");
        var admin = new Mock<IAdminClient>(MockBehavior.Strict);
        admin.Setup(x => x.GetMetadata("customers", TimeSpan.FromSeconds(5))).Returns(metadata);
        admin.Setup(x => x.Dispose());
        var factory = new Mock<IKafkaClientFactory>(MockBehavior.Strict);
        factory.Setup(x => x.CreateAdminClient()).Returns(admin.Object);
        var consumer = new Mock<IConsumer<string, byte[]>>(MockBehavior.Strict);
        consumer.Setup(x => x.Close());
        consumer.Setup(x => x.Dispose());
        factory.Setup(x => x.CreateConsumer<string>()).Returns(consumer.Object);
        var loader = new TopicWatermarkLoader(factory.Object, Options.Create(new TopicWatermarkLoaderConfiguration
        {
            AdminClientTimeout = TimeSpan.FromSeconds(5)
        }));
        var topic = new LoadingTopic(new TopicName("customers"), false, new DateFilterRange(null, null), EncoderRules.String, null);

        Func<Task> act = () => loader.LoadWatermarksAsync(topic, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage($"*customers*{error}*");
        admin.Verify(x => x.Dispose(), Times.Once);
        consumer.Verify(x => x.Close(), Times.Once);
        consumer.Verify(x => x.Dispose(), Times.Once);
        consumer.VerifyNoOtherCalls();
    }

    [Fact(DisplayName = "TopicWatermarkLoader honors prior cancellation without creating Kafka clients.")]
    public async Task PriorCancellationDoesNotCreateClients()
    {
        var factory = new Mock<IKafkaClientFactory>(MockBehavior.Strict);
        var loader = new TopicWatermarkLoader(factory.Object, Options.Create(new TopicWatermarkLoaderConfiguration()));
        var topic = new LoadingTopic(new TopicName("customers"), false, new DateFilterRange(null, null), EncoderRules.String, null);
        var consumerCreated = false;
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        Func<Task> act = () => loader.LoadWatermarksAsync(topic, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        consumerCreated.Should().BeFalse();
        factory.VerifyNoOtherCalls();
    }
}
