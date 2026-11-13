using Microsoft.Extensions.Logging;

using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

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

    private readonly ITopicWatermarkLoader _topicWatermarkLoader;
    private readonly ILogger _logger;
    private readonly IPartitionSnapshotBatchReader<TKey, TMessage> _partitionBatchReader;
    private readonly ISnapshotBuilder<TKey, TMessage> _snapshotBuilder;
}

