using Confluent.Kafka;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Import.Configuration;

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
                          IOptions<SnapshotLoaderConfiguration> config,
                          Func<IConsumer<TKey, byte[]>> consumerFactory,
                          ITopicWatermarkLoader topicWatermarkLoader,
                          IPartitionSnapshotBatchReader<TKey, TMessage> partitionBatchReader,
                          ISnapshotCompactor<TKey, TMessage> snapshotCompactor
                         )
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(config.Value, nameof(config));
        ArgumentNullException.ThrowIfNull(consumerFactory);
        ArgumentNullException.ThrowIfNull(topicWatermarkLoader);
        ArgumentNullException.ThrowIfNull(partitionBatchReader);
        ArgumentNullException.ThrowIfNull(snapshotCompactor);

        _logger = logger;
        _consumerFactory = consumerFactory;
        _topicWatermarkLoader = topicWatermarkLoader;
        _config = config.Value;
        _partitionBatchReader = partitionBatchReader;
        _snapshotCompactor = snapshotCompactor;

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
                                    .LoadWatermarksAsync(_consumerFactory, loadingTopic, ct)
                                    .ConfigureAwait(false);

        _logger.LogDebug("Loading initial state");
        var initialState = await ConsumeInitialAsync(
                                    topicWatermark,
                                    loadingTopic,
                                    keyFilter,
                                    valueFilter,
                                    ct)
                                .ConfigureAwait(false);

        _logger.LogDebug("Creating compacting state");

        return _snapshotCompactor.CreateSnapshot(initialState, loadingTopic.LoadWithCompacting);
    }

    private async Task<IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>>> ConsumeInitialAsync
       (TopicWatermark topicWatermark,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        if (_config.SearchSinglePartition)
        {
            return await _partitionBatchReader.ReadFirstNonEmptyAsync(
                topicWatermark.Watermarks,
                topicParams,
                keyFilter,
                valueFilter,
                ct).ConfigureAwait(false);
        }

        return await _partitionBatchReader.ReadAllAsync(
            topicWatermark.Watermarks,
            topicParams,
            keyFilter,
            valueFilter,
            ct).ConfigureAwait(false);
    }

    private readonly SnapshotLoaderConfiguration _config;
    private readonly Func<IConsumer<TKey, byte[]>> _consumerFactory;
    private readonly ITopicWatermarkLoader _topicWatermarkLoader;
    private readonly ILogger _logger;
    private readonly IPartitionSnapshotBatchReader<TKey, TMessage> _partitionBatchReader;
    private readonly ISnapshotCompactor<TKey, TMessage> _snapshotCompactor;
}

