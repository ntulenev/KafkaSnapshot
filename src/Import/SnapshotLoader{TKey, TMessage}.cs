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
using System.Collections;
using System.Text;

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
                          Func<IConsumer<TKey, TMessage>> consumerFactory,
                          ITopicWatermarkLoader topicWatermarkLoader,
                          IMessageSorter<TKey, TMessage> sorter
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
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(topicParams);
        ArgumentNullException.ThrowIfNull(keyFilter);
        ArgumentNullException.ThrowIfNull(valueFilter);

        _logger.LogDebug("Loading topic watermark");
        var topicWatermark = await _topicWatermarkLoader
                                    .LoadWatermarksAsync(_consumerFactory, topicParams, ct)
                                    .ConfigureAwait(false);

        _logger.LogDebug("Loading initial state");
        var initialState = await ConsumeInitialAsync(
                                    topicWatermark,
                                    topicParams,
                                    keyFilter,
                                    valueFilter,
                                    ct)
                                .ConfigureAwait(false);

        _logger.LogDebug("Creating compacting state");

        return CreateSnapshot(initialState, topicParams.LoadWithCompacting);
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

            return Enumerable.Empty<KeyValuePair<TKey, KafkaMessage<TMessage>>>();
        }
        else
        {
            var consumedEntities = await Task.WhenAll(topicWatermark.Watermarks
            .Select(watermark =>
                        Task.Run(() =>
                                    ConsumeToWatermark(
                                        watermark,
                                        topicParams,
                                        keyFilter,
                                        valueFilter,
                                        ct))
                   )
            ).ConfigureAwait(false);

            return consumedEntities.SelectMany(сonsumerResults => сonsumerResults);

        }
    }

    private IEnumerable<KeyValuePair<TKey, KafkaMessage<TMessage>>> ConsumeToWatermark(
        PartitionWatermark watermark,
        LoadingTopic topicParams,
        IDataFilter<TKey> keyFilter,
        IDataFilter<TMessage> valueFilter,
        CancellationToken ct)
    {
        using var _ = _logger.BeginScope("Partition {Partition}", watermark.Partition.Value);

        _logger.LogInformation("Watermarks: Low {Low}, High {High}",
                    watermark.Offset.Low,
                    watermark.Offset.High);

        using var consumer = _consumerFactory();

        try
        {

            if (topicParams.HasOffsetDate)
            {
                _logger.LogInformation("Searching for messages after date {Date}",
                            topicParams.OffsetDate);

                if (!watermark.AssingWithConsumer(
                            consumer,
                            topicParams.OffsetDate,
                            _config.DateOffsetTimeout))
                {
                    _logger.LogWarning("No actual offset for date {Date}", topicParams.OffsetDate);
                    yield break;
                }
            }
            else
            {
                watermark.AssingWithConsumer(consumer);
            }

            ConsumeResult<TKey, TMessage> result;

            bool isFinalOffsetDateReached() => topicParams.HasEndOffsetDate &&
                                               result.Message.Timestamp.UtcDateTime >
                                               topicParams.EndOffsetDate.ToUniversalTime();

            do
            {
                result = consumer.Consume(ct);

                if (isFinalOffsetDateReached())
                {
                    _logger.LogInformation("Final date offset {date} reached",
                                topicParams.EndOffsetDate);
                    break;
                }

                

                if (keyFilter.IsMatch(result.Message.Key) &&
                    valueFilter.IsMatch(result.Message.Value))
                {
                    _logger.LogTrace("Loading {Key} - {Value}",
                            result.Message.Key,
                            result.Message.Value);

                    var meta = new KafkaMetadata(
                            result.Message.Timestamp.UtcDateTime,
                            watermark.Partition.Value,
                            result.Offset.Value);

                    var message = new KafkaMessage<TMessage>(result.Message.Value, meta);

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
                });

            _logger.LogDebug("Created compacting state for {items} item(s)", result.Count());
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
    private readonly Func<IConsumer<TKey, TMessage>> _consumerFactory;
    private readonly ILogger<SnapshotLoader<TKey, TMessage>> _logger;
    private readonly IMessageSorter<TKey, TMessage> _sorter;
}

