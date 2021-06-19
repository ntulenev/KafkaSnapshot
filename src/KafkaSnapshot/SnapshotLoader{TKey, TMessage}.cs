using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaSnapshot.Metadata;
using KafkaSnapshot.Watermarks;
using KafkaSnapshot.Abstractions.Import;

namespace KafkaSnapshot
{
    ///<inheritdoc/>
    public class SnapshotLoader<TKey, TMessage> : ISnapshotLoader<TKey, TMessage> where TKey : notnull
    {
        /// <summary>
        /// Creates <see cref="SnapshotLoader{Key, Message}"/>.
        /// </summary>
        public SnapshotLoader(Func<IConsumer<TKey, TMessage>> consumerFactory,
                              ITopicWatermarkLoader topicWatermarkLoader
                             )
        {
            _topicWatermarkLoader = topicWatermarkLoader ?? throw new ArgumentNullException(nameof(topicWatermarkLoader));
            _consumerFactory = consumerFactory ?? throw new ArgumentNullException(nameof(consumerFactory));
        }

        ///<inheritdoc/>
        public async Task<IDictionary<TKey, TMessage>> LoadCompactSnapshotAsync(CancellationToken ct)
        {
            var topicWatermark = await _topicWatermarkLoader.LoadWatermarksAsync(_consumerFactory, ct).ConfigureAwait(false);
            var initialState = await ConsumeInitialAsync(topicWatermark, ct).ConfigureAwait(false);
            var compactedState = CreateSnapshot(initialState);
            return compactedState;
        }

        private async Task<IEnumerable<KeyValuePair<TKey, TMessage>>> ConsumeInitialAsync
           (TopicWatermark topicWatermark,
           CancellationToken ct)
        {
            var consumedEntities = await Task.WhenAll(topicWatermark.Watermarks
                .Select(watermark =>
                            Task.Run(() => ConsumeFromWatermark(watermark, ct))
                       )
                ).ConfigureAwait(false);

            return consumedEntities.SelectMany(сonsumerResults => сonsumerResults);
        }

        private IEnumerable<KeyValuePair<TKey, TMessage>> ConsumeFromWatermark(PartitionWatermark watermark, CancellationToken ct)
        {
            using var consumer = _consumerFactory();
            try
            {
                watermark.AssingWithConsumer(consumer);
                ConsumeResult<TKey, TMessage> result = default!;
                do
                {
                    result = consumer.Consume(ct);
                    // TODO Add Desirializer for result.Message.Value depend on topic
                    yield return new KeyValuePair<TKey, TMessage>(result.Message.Key, result.Message.Value);

                } while (watermark.IsWatermarkAchievedBy(result));
            }
            finally
            {
                consumer?.Close();
            }
        }

        private static IDictionary<TKey, TMessage> CreateSnapshot(IEnumerable<KeyValuePair<TKey, TMessage>> items)
        {
            var itemssss = items.ToList();
            return items.Where(x => x.Key is not null).Aggregate(
                new Dictionary<TKey, TMessage>(),
                (d, e) =>
                {
                    d[e.Key] = e.Value;
                    return d;
                });
        }

        private readonly ITopicWatermarkLoader _topicWatermarkLoader;
        private readonly Func<IConsumer<TKey, TMessage>> _consumerFactory;
    }
}

