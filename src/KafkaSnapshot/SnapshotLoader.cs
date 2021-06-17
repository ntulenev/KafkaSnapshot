using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaSnapshot.Metadata;
using KafkaSnapshot.Watermarks;

namespace KafkaSnapshot
{
    public class SnapshotLoader<Key, Message> : ISnapshotLoader<Key, Message> where Key : notnull
    {

        public SnapshotLoader(Func<IConsumer<Key, Message>> consumerFactory,
                              ITopicWatermarkLoader topicWatermarkLoader
                             )
        {
            _topicWatermarkLoader = topicWatermarkLoader ?? throw new ArgumentNullException(nameof(topicWatermarkLoader));
            _consumerFactory = consumerFactory ?? throw new ArgumentNullException(nameof(consumerFactory));
        }

        public async Task<IDictionary<Key, Message>> LoadCompactSnapshotAsync(CancellationToken ct)
        {
            var topicWatermark = await _topicWatermarkLoader.LoadWatermarksAsync(_consumerFactory, ct).ConfigureAwait(false);
            var initialState = await ConsumeInitialAsync(topicWatermark, ct).ConfigureAwait(false);
            var compactedState = CreateSnapshot(initialState);
            return compactedState;
        }

        private async Task<IEnumerable<KeyValuePair<Key, Message>>> ConsumeInitialAsync
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

        private IEnumerable<KeyValuePair<Key, Message>> ConsumeFromWatermark(PartitionWatermark watermark, CancellationToken ct)
        {
            using var consumer = _consumerFactory();
            try
            {
                watermark.AssingWithConsumer(consumer);
                ConsumeResult<Key, Message> result = default!;
                do
                {
                    result = consumer.Consume(ct);
                    // TODO Add Desirializer for result.Message.Value depend on topic
                    yield return new KeyValuePair<Key, Message>(result.Message.Key, result.Message.Value);

                } while (watermark.IsWatermarkAchievedBy(result));
            }
            finally
            {
                consumer?.Close();
            }
        }

        private static IDictionary<Key, Message> CreateSnapshot(IEnumerable<KeyValuePair<Key, Message>> items)
        {
            var itemssss = items.ToList();
            return items.Where(x => x.Key is not null).Aggregate(
                new Dictionary<Key, Message>(),
                (d, e) =>
                {
                    d[e.Key] = e.Value;
                    return d;
                });
        }

        private readonly ITopicWatermarkLoader _topicWatermarkLoader;
        private readonly Func<IConsumer<Key, Message>> _consumerFactory;
    }
}

