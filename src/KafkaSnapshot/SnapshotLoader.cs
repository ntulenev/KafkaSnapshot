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
    public class SnapshotLoader<Message, Key, Value> where Key : notnull
    {

        public SnapshotLoader(Func<Message, (Key, Value)> deserializer,
                              Func<IConsumer<Ignore, Message>> consumerFactory,
                              ITopicWatermarkLoader topicWatermarkLoader
                             )
        {
            _topicWatermarkLoader = topicWatermarkLoader ?? throw new ArgumentNullException(nameof(topicWatermarkLoader));
            _consumerFactory = consumerFactory ?? throw new ArgumentNullException(nameof(consumerFactory));
            _deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));
        }

        public async Task<IEnumerable<Value>> LoadCompactSnapshotAsync(CancellationToken ct)
        {
            var topicWatermark = await _topicWatermarkLoader.LoadWatermarksAsync(_consumerFactory, ct);
            var initialState = await ConsumeInitialAsync(topicWatermark, ct);
            return initialState.Select(x => x.Value).ToList();
        }

        private async Task<IEnumerable<KeyValuePair<Key, Value>>> ConsumeInitialAsync
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

        private IEnumerable<KeyValuePair<Key, Value>> ConsumeFromWatermark(PartitionWatermark watermark, CancellationToken ct)
        {
            using var consumer = _consumerFactory();
            try
            {
                watermark.AssingWithConsumer(consumer);
                ConsumeResult<Ignore, Message> result = default!;
                do
                {
                    result = consumer.Consume(ct);
                    var (key, value) = _deserializer(result.Message.Value);
                    yield return new KeyValuePair<Key, Value>(key, value);

                } while (watermark.IsWatermarkAchievedBy(result));
            }
            finally
            {
                consumer?.Close();
            }
        }


        private IDictionary<Key, Value> CreateSnapshot(IEnumerable<KeyValuePair<Key, Value>> items)
        {
            return items.Aggregate(
                new Dictionary<Key, Value>(),
                (d, e) =>
                {
                    d[e.Key] = e.Value;
                    return d;
                });
        }

        private readonly ITopicWatermarkLoader _topicWatermarkLoader;
        private readonly Func<IConsumer<Ignore, Message>> _consumerFactory;
        private readonly Func<Message, (Key, Value)> _deserializer;
    }
}

