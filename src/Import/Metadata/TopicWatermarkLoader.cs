using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaSnapshot.Import.Watermarks;

namespace KafkaSnapshot.Import.Metadata
{
    /// <summary>
    /// Service that loads <see cref="TopicWatermark"/>.
    /// </summary>
    public class TopicWatermarkLoader : ITopicWatermarkLoader
    {
        /// <summary>
        /// Creates <see cref="TopicWatermarkLoader"/>.
        /// </summary>
        /// <param name="topicName">Topic name.</param>
        /// <param name="adminClient">Kafla admin client.</param>
        /// <param name="intTimeoutSeconds">Timeout in seconds for loading watermarks.</param>
        public TopicWatermarkLoader(TopicName topicName,
                                    IAdminClient adminClient,
                                    int intTimeoutSeconds)
        {
            if (topicName is null)
            {
                throw new ArgumentNullException(nameof(topicName));
            }

            if (adminClient is null)
            {
                throw new ArgumentNullException(nameof(adminClient));
            }

            if (intTimeoutSeconds <= 0)
            {
                throw new ArgumentException(
                    "The watermark timeout should be positive.", nameof(intTimeoutSeconds));
            }

            _intTimeoutSeconds = intTimeoutSeconds;
            _adminClient = adminClient;
            _topicName = topicName;
        }

        private IEnumerable<TopicPartition> SplitTopicOnPartitions()
        {
            var topicMeta = _adminClient.GetMetadata(_topicName.Value, TimeSpan.FromSeconds(_intTimeoutSeconds));

            var partitions = topicMeta.Topics.Single().Partitions;

            return partitions.Select(partition => new TopicPartition(_topicName.Value, new Partition(partition.PartitionId)));
        }

        private PartitionWatermark CreatePartitionWatermark<Key, Value>(IConsumer<Key, Value> consumer, TopicPartition topicPartition)
        {
            var watermarkOffsets = consumer.QueryWatermarkOffsets(
                                    topicPartition,
                                    TimeSpan.FromSeconds(_intTimeoutSeconds));

            return new PartitionWatermark(_topicName, watermarkOffsets, topicPartition.Partition);
        }

        /// <inheritdoc/>>
        public async Task<TopicWatermark> LoadWatermarksAsync<Key, Value>(Func<IConsumer<Key, Value>> consumerFactory, CancellationToken ct)
        {
            if (consumerFactory is null)
            {
                throw new ArgumentNullException(nameof(consumerFactory));
            }

            using var consumer = consumerFactory();

            try
            {
                var partitions = SplitTopicOnPartitions();

                var partitionWatermarks = await Task.WhenAll(partitions.Select(
                            topicPartition => Task.Run(() =>
                            CreatePartitionWatermark(consumer, topicPartition), ct)
                                                       )).ConfigureAwait(false);

                return new TopicWatermark(partitionWatermarks.Where(item => item.IsReadyToRead()));
            }
            finally
            {
                consumer.Close();
            }
        }

        private readonly TopicName _topicName;
        private readonly IAdminClient _adminClient;
        private readonly int _intTimeoutSeconds;
    }
}
