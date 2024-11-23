using Confluent.Kafka;

using KafkaSnapshot.Import.Configuration;
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
    /// <param name="adminClient">Kafka admin client.</param>
    /// <param name="intTimeoutSeconds">Timeout in seconds for loading watermarks.</param>
    public TopicWatermarkLoader(IAdminClient adminClient,
                                IOptions<TopicWatermarkLoaderConfiguration> options)
    {
        ArgumentNullException.ThrowIfNull(adminClient);
        ArgumentNullException.ThrowIfNull(options);

        if (options.Value is null)
        {
            throw new ArgumentException("Options value is not set", nameof(options));
        }

        _metaTimeout = options.Value.AdminClientTimeout;
        _adminClient = adminClient;
    }

    private IEnumerable<TopicPartition> SplitTopicOnPartitions(LoadingTopic loadingTopic)
    {
        var topicMeta = _adminClient.GetMetadata(loadingTopic.Value.Name, _metaTimeout);

        IEnumerable<PartitionMetadata> partitions = topicMeta.Topics.Single().Partitions;

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

    /// <inheritdoc/>>
    public async Task<TopicWatermark> LoadWatermarksAsync<TKey, TValue>(
                        Func<IConsumer<TKey, TValue>> consumerFactory,
                        LoadingTopic loadingTopic,
                        CancellationToken ct
                        )
    {
        ArgumentNullException.ThrowIfNull(consumerFactory);
        ArgumentNullException.ThrowIfNull(loadingTopic);

        using var consumer = consumerFactory();

        try
        {
            var partitions = SplitTopicOnPartitions(loadingTopic);

            var partitionWatermarks = await Task.WhenAll(partitions.Select(
                        topicPartition => Task.Run(() =>
                        CreatePartitionWatermark(consumer, loadingTopic, topicPartition), ct)
                                                   )).ConfigureAwait(false);

            return new TopicWatermark(partitionWatermarks.Where(item => item.IsReadyToRead()));
        }
        finally
        {
            consumer.Close();
        }
    }

    private readonly IAdminClient _adminClient;
    private readonly TimeSpan _metaTimeout;
}
