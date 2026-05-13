using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Import;

/// <summary>
/// Reads Kafka messages from multiple topic partitions.
/// </summary>
/// <typeparam name="TKey">Kafka message key type.</typeparam>
/// <typeparam name="TMessage">Decoded Kafka message value type.</typeparam>
public class PartitionSnapshotBatchReader<TKey, TMessage>
    : IPartitionSnapshotBatchReader<TKey, TMessage>
    where TKey : notnull
    where TMessage : notnull
{
    /// <summary>
    /// Creates <see cref="PartitionSnapshotBatchReader{TKey, TMessage}"/>.
    /// </summary>
    /// <param name="config">Snapshot loader configuration.</param>
    /// <param name="partitionReader">Single partition reader.</param>
    public PartitionSnapshotBatchReader(
        IOptions<SnapshotLoaderConfiguration> config,
        IPartitionSnapshotReader<TKey, TMessage> partitionReader)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(config.Value, nameof(config));
        ArgumentNullException.ThrowIfNull(partitionReader);

        _config = config.Value;
        _partitionReader = partitionReader;
    }

    /// <summary>
    /// Reads partitions one by one until the first non-empty partition snapshot is found.
    /// </summary>
    /// <param name="watermarks">Partition watermarks.</param>
    /// <param name="topicParams">Topic loading parameters.</param>
    /// <param name="keyFilter">Message key filter.</param>
    /// <param name="valueFilter">Message value filter.</param>
    /// <param name="ct">Cancellation token.</param>
    public Task<IReadOnlyCollection<KeyValuePair<TKey, KafkaMessage<TMessage>>>> ReadFirstNonEmptyAsync(
        IEnumerable<PartitionWatermark> watermarks,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(watermarks);
        ArgumentNullException.ThrowIfNull(topicParams);
        ArgumentNullException.ThrowIfNull(keyFilter);
        ArgumentNullException.ThrowIfNull(valueFilter);

        foreach (var watermark in watermarks)
        {
            var result = ReadPartition(
                watermark,
                topicParams,
                keyFilter,
                valueFilter,
                ct);

            if (result.Count > 0)
            {
                return Task.FromResult<IReadOnlyCollection<KeyValuePair<TKey, KafkaMessage<TMessage>>>>(result);
            }
        }

        return Task.FromResult<IReadOnlyCollection<KeyValuePair<TKey, KafkaMessage<TMessage>>>>([]);
    }

    /// <summary>
    /// Reads all partition snapshots with configured concurrency.
    /// </summary>
    /// <param name="watermarks">Partition watermarks.</param>
    /// <param name="topicParams">Topic loading parameters.</param>
    /// <param name="keyFilter">Message key filter.</param>
    /// <param name="valueFilter">Message value filter.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<IReadOnlyCollection<KeyValuePair<TKey, KafkaMessage<TMessage>>>> ReadAllAsync(
        IEnumerable<PartitionWatermark> watermarks,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(watermarks);
        ArgumentNullException.ThrowIfNull(topicParams);
        ArgumentNullException.ThrowIfNull(keyFilter);
        ArgumentNullException.ThrowIfNull(valueFilter);

        var partitionWatermarks = watermarks.ToList();

        if (partitionWatermarks.Count == 0)
        {
            return [];
        }

        using var concurrency = new SemaphoreSlim(ResolveMaxConcurrency(partitionWatermarks.Count));

#pragma warning disable CA2025 // Task.WhenAll below completes all users of the semaphore before disposal.
        var tasks = partitionWatermarks
            .Select(watermark => ReadPartitionOnWorkerAsync(
                watermark,
                topicParams,
                keyFilter,
                valueFilter,
                concurrency,
                ct))
            .ToArray();
#pragma warning restore CA2025

        var consumedEntities = await Task.WhenAll(tasks).ConfigureAwait(false);

        return [.. consumedEntities.SelectMany(consumerResults => consumerResults)];
    }

    private int ResolveMaxConcurrency(int partitionCount)
    {
        var maxConcurrency = _config.MaxConcurrentPartitions.GetValueOrDefault();

        return maxConcurrency < 1
            ? partitionCount
            : Math.Min(maxConcurrency, partitionCount);
    }

    private async Task<List<KeyValuePair<TKey, KafkaMessage<TMessage>>>> ReadPartitionOnWorkerAsync(
        PartitionWatermark watermark,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        SemaphoreSlim concurrency,
        CancellationToken ct)
    {
        await concurrency.WaitAsync(ct).ConfigureAwait(false);

        try
        {
            // Confluent Kafka consumer.Consume is blocking, so partition reads run on worker threads.
            return await Task.Run(
                () => ReadPartition(
                    watermark,
                    topicParams,
                    keyFilter,
                    valueFilter,
                    ct),
                ct).ConfigureAwait(false);
        }
        finally
        {
            _ = concurrency.Release();
        }
    }

    private List<KeyValuePair<TKey, KafkaMessage<TMessage>>> ReadPartition(
        PartitionWatermark watermark,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
        => [.. _partitionReader.Read(
                watermark,
                topicParams,
                keyFilter,
                valueFilter,
                ct)];

    private readonly SnapshotLoaderConfiguration _config;
    private readonly IPartitionSnapshotReader<TKey, TMessage> _partitionReader;
}
