using System.Runtime.CompilerServices;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

using Microsoft.Extensions.Logging;

namespace KafkaSnapshot.Import;

///<inheritdoc/>
public class SnapshotLoader<TKey, TMessage> : ISnapshotLoader<TKey, TMessage>
    where TKey : notnull
    where TMessage : notnull
{
    /// <summary>
    /// Creates <see cref="SnapshotLoader{TKey, TMessage}"/>.
    /// </summary>
    public SnapshotLoader(ILogger<SnapshotLoader<TKey, TMessage>> logger,
                          ITopicWatermarkLoader topicWatermarkLoader,
                          IPartitionSnapshotBatchReader<TKey, TMessage> partitionBatchReader,
                          ISnapshotBuilder<TKey, TMessage> snapshotBuilder
                         )
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(topicWatermarkLoader);
        ArgumentNullException.ThrowIfNull(partitionBatchReader);
        ArgumentNullException.ThrowIfNull(snapshotBuilder);

        _logger = logger;
        _topicWatermarkLoader = topicWatermarkLoader;
        _partitionBatchReader = partitionBatchReader;
        _snapshotBuilder = snapshotBuilder;

        _logger.LogDebug("Instance created");
    }

    ///<inheritdoc/>
    public async Task<IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>>> LoadSnapshotAsync(
        LoadingTopic loadingTopic,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(loadingTopic);
        ArgumentNullException.ThrowIfNull(keyFilter);
        ArgumentNullException.ThrowIfNull(valueFilter);

        _logger.LogDebug("Loading topic watermark");
        var topicWatermark = await _topicWatermarkLoader
                                    .LoadWatermarksAsync(loadingTopic, ct)
                                    .ConfigureAwait(false);

        _logger.LogDebug("Loading initial state");
        var initialState = await _partitionBatchReader.ReadAsync(
                                    topicWatermark.Watermarks,
                                    loadingTopic,
                                    keyFilter,
                                    valueFilter,
                                    ct)
                                .ConfigureAwait(false);

        _logger.LogDebug("Creating compacting state");

        return _snapshotBuilder.CreateSnapshot(initialState, loadingTopic.LoadWithCompacting);
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> StreamSnapshotAsync(
        LoadingTopic loadingTopic,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        [EnumeratorCancellation] CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(loadingTopic);
        ArgumentNullException.ThrowIfNull(keyFilter);
        ArgumentNullException.ThrowIfNull(valueFilter);
        ct.ThrowIfCancellationRequested();

        if (_snapshotBuilder.RequiresBuffering(loadingTopic.LoadWithCompacting))
        {
            var snapshot = await LoadSnapshotAsync(loadingTopic, keyFilter, valueFilter, ct)
                .ConfigureAwait(false);
            foreach (var item in snapshot)
            {
                ct.ThrowIfCancellationRequested();
                yield return item;
            }

            yield break;
        }

        var watermark = await _topicWatermarkLoader.LoadWatermarksAsync(loadingTopic, ct)
            .ConfigureAwait(false);
        await foreach (var item in _partitionBatchReader.StreamAsync(watermark.Watermarks,
            loadingTopic, keyFilter, valueFilter, ct).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private readonly ITopicWatermarkLoader _topicWatermarkLoader;
    private readonly ILogger _logger;
    private readonly IPartitionSnapshotBatchReader<TKey, TMessage> _partitionBatchReader;
    private readonly ISnapshotBuilder<TKey, TMessage> _snapshotBuilder;
}

