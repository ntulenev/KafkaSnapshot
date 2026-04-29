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
using KafkaSnapshot.Abstractions.Sorting;

using System.Collections.Frozen;

namespace KafkaSnapshot.Import;

///<inheritdoc/>
public class SnapshotLoader<TKey, TMessage> : ISnapshotLoader<TKey, TMessage>
    where TKey : notnull
    where TMessage : notnull
{
    /// <summary>
    /// Creates <see cref="SnapshotLoader{Key, Message}"/>.
    /// </summary>
    public SnapshotLoader(ILogger<SnapshotLoader<TKey, TMessage>> logger,
                          IOptions<SnapshotLoaderConfiguration> config,
                          Func<IConsumer<TKey, byte[]>> consumerFactory,
                          ITopicWatermarkLoader topicWatermarkLoader,
                          IMessageSorter<TKey, TMessage> sorter,
                          IMessageEncoder<byte[], TMessage> encoder
                         )
    {
        _logger = logger
            ?? throw new ArgumentNullException(nameof(logger));
        _topicWatermarkLoader = topicWatermarkLoader
            ?? throw new ArgumentNullException(nameof(topicWatermarkLoader));
        _consumerFactory = consumerFactory
            ?? throw new ArgumentNullException(nameof(consumerFactory));
        _sorter = sorter
            ?? throw new ArgumentNullException(nameof(sorter));
        _encoder = encoder
            ?? throw new ArgumentNullException(nameof(encoder));

        ArgumentNullException.ThrowIfNull(config);

        if (config.Value is null)
        {
            throw new ArgumentException("Config is not set", nameof(config));
        }

        _config = config.Value;

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

        return CreateSnapshot(initialState, loadingTopic.LoadWithCompacting);
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
            foreach (var watermark in topicWatermark.Watermarks)
            {
                var result = ConsumeToWatermark(
                                watermark,
                                topicParams,
                                keyFilter,
                                valueFilter,
                                ct);

                if (result.Any())
                {
                    return result;
                }
            }

            return [];
        }
        return await ConsumePartitionsAsync(
            topicWatermark.Watermarks,
            topicParams,
            keyFilter,
            valueFilter,
            ct).ConfigureAwait(false);
    }

    private async Task<IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>>> ConsumePartitionsAsync(
        IEnumerable<PartitionWatermark> watermarks,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        var partitionWatermarks = watermarks.ToList();

        if (partitionWatermarks.Count == 0)
        {
            return [];
        }

        var maxConcurrency = _config.MaxConcurrentPartitions.GetValueOrDefault();

        if (maxConcurrency < 1)
        {
            maxConcurrency = partitionWatermarks.Count;
        }

        using var concurrency = new SemaphoreSlim(maxConcurrency);

        var tasks = partitionWatermarks.Select(async watermark =>
        {
            await concurrency.WaitAsync(ct).ConfigureAwait(false);

            try
            {
                return await Task.Run(
                    () => ConsumeToWatermark(
                        watermark,
                        topicParams,
                        keyFilter,
                        valueFilter,
                        ct).ToList(),
                    ct).ConfigureAwait(false);
            }
            finally
            {
                _ = concurrency.Release();
            }
        });

        var consumedEntities = await Task.WhenAll(tasks).ConfigureAwait(false);

        return consumedEntities.SelectMany(consumerResults => consumerResults);
    }

    private IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> ConsumeToWatermark(
        PartitionWatermark watermark,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        var logScope = _logger.IsEnabled(LogLevel.Information)
            ? _logger.BeginScope("Partition {Partition}", watermark.Partition.Value)
            : null;

        using (logScope)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Watermarks: Low {Low}, High {High}",
                    watermark.Offset.Low,
                    watermark.Offset.High);
            }

            using var consumer = _consumerFactory();

            try
            {

                if (topicParams.HasOffsetDate)
                {
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("Searching for messages after date {Date}",
                            topicParams.OffsetDate);
                    }

                    if (!watermark.AssignWithConsumer(
                                consumer,
                                topicParams.OffsetDate,
                                _config.DateOffsetTimeout))
                    {
                        if (_logger.IsEnabled(LogLevel.Warning))
                        {
                            _logger.LogWarning("No actual offset for date {Date}", topicParams.OffsetDate);
                        }

                        yield break;
                    }
                }
                else
                {
                    watermark.AssignWithConsumer(consumer);
                }

                ConsumeResult<TKey, byte[]> result;

                bool isFinalOffsetDateReached()
                {
                    return topicParams.HasEndOffsetDate &&
                           result.Message.Timestamp.UtcDateTime >
                           topicParams.EndOffsetDate.ToUniversalTime();
                }

                do
                {
                    result = consumer.Consume(ct);

                    if (isFinalOffsetDateReached())
                    {
                        if (_logger.IsEnabled(LogLevel.Information))
                        {
                            _logger.LogInformation("Final date offset {Date} reached",
                                topicParams.EndOffsetDate);
                        }

                        break;
                    }

                    var messageValue = _encoder.Encode(result.Message.Value, topicParams.TopicValueEncoderRule);

                    if (keyFilter.IsMatch(result.Message.Key) &&
                        valueFilter.IsMatch(messageValue))
                    {
                        if (_logger.IsEnabled(LogLevel.Trace))
                        {
                            _logger.LogTrace("Loading {Key} - {Value}",
                                result.Message.Key,
                                messageValue);
                        }

                        var meta = new KafkaMetadata(
                                result.Message.Timestamp.UtcDateTime,
                                watermark.Partition.Value,
                                result.Offset.Value);

                        var message = new KafkaMessage<TMessage>(messageValue, meta);

                        yield return new KeyValuePair<TKey, KafkaMessage<TMessage>>(
                            result.Message.Key,
                            message);
                    }

                } while (watermark.IsWatermarkAchievedBy(result));
            }
            finally
            {
                consumer?.Close();
            }
        }
    }

    private IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> CreateSnapshot(
                IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> items,
                bool withCompacting)
    {
        IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> result = null!;

        if (withCompacting)
        {
            _logger.LogDebug("Compacting data");

            result = items.Where(x => x.Key is not null).Aggregate(
                new Dictionary<TKey, KafkaMessage<TMessage>>(),
                (d, e) =>
                {
                    d[e.Key] = e.Value;
                    return d;
                }).ToFrozenDictionary();

            if (_logger.IsEnabled(LogLevel.Debug))
            {
                var itemCount = result.Count();
                _logger.LogDebug("Created compacting state for {ItemCount} item(s)", itemCount);
            }
        }
        else
        {
            result = _sorter.Sort(items);

            _logger.LogDebug("Created state without compacting");
        }

        return result;
    }

    private readonly SnapshotLoaderConfiguration _config;
    private readonly ITopicWatermarkLoader _topicWatermarkLoader;
    private readonly Func<IConsumer<TKey, byte[]>> _consumerFactory;
    private readonly ILogger _logger;
    private readonly IMessageSorter<TKey, TMessage> _sorter;
    private readonly IMessageEncoder<byte[], TMessage> _encoder;
}

