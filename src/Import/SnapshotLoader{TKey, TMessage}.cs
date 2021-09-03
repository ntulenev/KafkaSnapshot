using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Import
{
    ///<inheritdoc/>
    public class SnapshotLoader<TKey, TMessage> : ISnapshotLoader<TKey, TMessage>
        where TKey : notnull
        where TMessage : notnull
    {
        /// <summary>
        /// Creates <see cref="SnapshotLoader{Key, Message}"/>.
        /// </summary>
        public SnapshotLoader(ILogger<SnapshotLoader<TKey, TMessage>> logger,
                              Func<IConsumer<TKey, TMessage>> consumerFactory,
                              ITopicWatermarkLoader topicWatermarkLoader
                             )
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _topicWatermarkLoader = topicWatermarkLoader ?? throw new ArgumentNullException(nameof(topicWatermarkLoader));
            _consumerFactory = consumerFactory ?? throw new ArgumentNullException(nameof(consumerFactory));

            _logger.LogDebug("Instance created.");
        }

        ///<inheritdoc/>
        public async Task<IEnumerable<KeyValuePair<TKey, DatedMessage<TMessage>>>> LoadCompactSnapshotAsync(
            LoadTopicParams topicParams,
            IKeyFilter<TKey> filter,
            CancellationToken ct)
        {
            if (topicParams is null)
            {
                throw new ArgumentNullException(nameof(topicParams));
            }

            if (filter is null)
            {
                throw new ArgumentNullException(nameof(filter));
            }

            _logger.LogDebug("Loading topic watermark.");
            var topicWatermark = await _topicWatermarkLoader
                                        .LoadWatermarksAsync(_consumerFactory, topicParams, ct)
                                        .ConfigureAwait(false);

            _logger.LogDebug("Loading initial state.");
            var initialState = await ConsumeInitialAsync(topicWatermark, topicParams, filter, ct).ConfigureAwait(false);

            _logger.LogDebug("Creating compacting state.");
            var compactedState = CreateSnapshot(initialState, topicParams.LoadWithCompacting);

            _logger.LogDebug("Created compacting state for {items} item(s).", compactedState.Count());

            return compactedState;
        }

        private async Task<IEnumerable<KeyValuePair<TKey, DatedMessage<TMessage>>>> ConsumeInitialAsync
           (TopicWatermark topicWatermark,
            LoadTopicParams topicParams,
            IKeyFilter<TKey> filter,
            CancellationToken ct)
        {
            var consumedEntities = await Task.WhenAll(topicWatermark.Watermarks
                .Select(watermark =>
                            Task.Run(() => ConsumeToWatermark(watermark, topicParams, filter, ct))
                       )
                ).ConfigureAwait(false);

            return consumedEntities.SelectMany(сonsumerResults => сonsumerResults);
        }

        private IEnumerable<KeyValuePair<TKey, DatedMessage<TMessage>>> ConsumeToWatermark(
            PartitionWatermark watermark,
            LoadTopicParams topicParams,
            IKeyFilter<TKey> filter,
            CancellationToken ct)
        {
            using var consumer = _consumerFactory();
            try
            {

                if (topicParams.HasOffsetDate)
                {
                    watermark.AssingWithConsumer(consumer, topicParams.OffsetDate, /*Add to config*/ TimeSpan.FromSeconds(10));
                }
                else
                {
                    watermark.AssingWithConsumer(consumer);
                }

                ConsumeResult<TKey, TMessage> result;
                do
                {
                    result = consumer.Consume(ct);

                    if (filter.IsMatch(result.Message.Key))
                    {
                        _logger.LogTrace("Loading {Key} - {Value}", result.Message.Key, result.Message.Value);
                        var message = new DatedMessage<TMessage>(result.Message.Value, result.Message.Timestamp.UtcDateTime);
                        yield return new KeyValuePair<TKey, DatedMessage<TMessage>>(result.Message.Key, message);
                    }

                } while (watermark.IsWatermarkAchievedBy(result));
            }
            finally
            {
                consumer?.Close();
            }
        }

        private IEnumerable<KeyValuePair<TKey, DatedMessage<TMessage>>> CreateSnapshot(IEnumerable<KeyValuePair<TKey, DatedMessage<TMessage>>> items, bool withCompacting)
        {
            if (withCompacting)
            {
                _logger.LogDebug("Compacting data.");

                return items.Where(x => x.Key is not null).Aggregate(
                    new Dictionary<TKey, DatedMessage<TMessage>>(),
                    (d, e) =>
                    {
                        d[e.Key] = e.Value;
                        return d;
                    });
            }
            else
            {
                return items.ToList();
            }
        }

        private readonly ITopicWatermarkLoader _topicWatermarkLoader;
        private readonly Func<IConsumer<TKey, TMessage>> _consumerFactory;
        private readonly ILogger<SnapshotLoader<TKey, TMessage>> _logger;
    }
}

