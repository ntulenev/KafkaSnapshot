using Confluent.Kafka;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Import;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import.Metadata;

/// <summary>
/// Service that loads <see cref="TopicWatermark"/>.
/// </summary>
public class TopicWatermarkLoader : ITopicWatermarkLoader
{
    /// <summary>
    /// Creates <see cref="TopicWatermarkLoader"/>.
    /// </summary>
    /// <param name="kafkaClientFactory">Kafka client factory.</param>
    /// <param name="options">Loader options.</param>
    public TopicWatermarkLoader(IKafkaClientFactory kafkaClientFactory,
                                IOptions<TopicWatermarkLoaderConfiguration> options)
    {
        ArgumentNullException.ThrowIfNull(kafkaClientFactory);
        ArgumentNullException.ThrowIfNull(options);

        if (options.Value is null)
        {
            throw new ArgumentException("Options value is not set", nameof(options));
        }

        _metaTimeout = options.Value.AdminClientTimeout;
        _kafkaClientFactory = kafkaClientFactory;
    }

    private IEnumerable<TopicPartition> SplitTopicOnPartitions(LoadingTopic loadingTopic)
    {
        var requestedTopicName = loadingTopic.Value.Name;
        using var adminClient = _kafkaClientFactory.CreateAdminClient();
        var metadata = adminClient.GetMetadata(requestedTopicName, _metaTimeout);
        var topicMetas = metadata.Topics
                                 .Where(topic => topic.Topic == requestedTopicName)
                                 .ToList();

        if (topicMetas.Count == 0)
        {
            throw new InvalidOperationException(
                $"Metadata for topic '{requestedTopicName}' was not found.");
        }

        if (topicMetas.Count > 1)
        {
            throw new InvalidOperationException(
                $"Metadata for topic '{requestedTopicName}' is ambiguous.");
        }

        var topicMeta = topicMetas[0];

        if (topicMeta.Error is not null && topicMeta.Error.IsError)
        {
            throw new InvalidOperationException(
                $"Metadata for topic '{requestedTopicName}' returned error " +
                $"{topicMeta.Error.Code}: {topicMeta.Error.Reason}");
        }

        if (topicMeta.Partitions is null)
        {
            throw new InvalidOperationException(
                $"Metadata for topic '{requestedTopicName}' does not contain partitions.");
        }

        IEnumerable<PartitionMetadata> partitions = topicMeta.Partitions;

        if (loadingTopic.HasPartitionFilter)
        {
            partitions = partitions.Where(
                x => loadingTopic.PartitionFilter.Contains(x.PartitionId));
        }

        return partitions.Select(
            partition =>
                new TopicPartition(
                    loadingTopic.Value.Name,
                    new Partition(partition.PartitionId)));
    }

    private PartitionWatermark CreatePartitionWatermark<Key, Value>
        (IConsumer<Key, Value> consumer,
        LoadingTopic topicName,
        TopicPartition topicPartition)
    {
        var watermarkOffsets = consumer.QueryWatermarkOffsets(
                                topicPartition,
                                _metaTimeout);

        return new PartitionWatermark(topicName, watermarkOffsets, topicPartition.Partition);
    }

    /// <inheritdoc />
    public Task<TopicWatermark> LoadWatermarksAsync<TKey, TValue>(
                        Func<IConsumer<TKey, TValue>> consumerFactory,
                        LoadingTopic loadingTopic,
                        CancellationToken ct
                        )
    {
        ArgumentNullException.ThrowIfNull(consumerFactory);
        ArgumentNullException.ThrowIfNull(loadingTopic);

        ct.ThrowIfCancellationRequested();

        using var consumer = consumerFactory();

        try
        {
            var partitions = SplitTopicOnPartitions(loadingTopic);

            var partitionWatermarks = partitions
                .Select(topicPartition =>
                {
                    ct.ThrowIfCancellationRequested();
                    return CreatePartitionWatermark(consumer, loadingTopic, topicPartition);
                })
                .ToList();

            return Task.FromResult(
                new TopicWatermark(partitionWatermarks.Where(item => item.IsReadyToRead())));
        }
        finally
        {
            consumer.Close();
        }
    }

    private readonly IKafkaClientFactory _kafkaClientFactory;
    private readonly TimeSpan _metaTimeout;
}
