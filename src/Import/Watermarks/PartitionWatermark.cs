﻿using System;
using System.Linq;

using Confluent.Kafka;

using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Import.Watermarks
{
    /// <summary>
    /// Offset watermark for single partition in topic.
    /// </summary>
    public class PartitionWatermark
    {
        public Partition Partition => _partition;

        public WatermarkOffsets Offset => _offset;

        public LoadingTopic TopicName => _topicName;

        /// <summary>
        /// Creates partition offset watermark.
        /// </summary>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="offset">Raw kafka offset representation.</param>
        /// <param name="partition">Raw kafka partition representation.</param>
        public PartitionWatermark(LoadingTopic topicName,
                                  WatermarkOffsets offset,
                                  Partition partition)
        {
            if (topicName is null)
            {
                throw new ArgumentNullException(nameof(topicName));
            }

            if (offset is null)
            {
                throw new ArgumentNullException(nameof(offset));
            }

            _topicName = topicName;

            _offset = offset;

            _partition = partition;
        }

        /// <summary>
        /// Checks that partition if valid for reading.
        /// </summary>
        public bool IsReadyToRead() => _offset.High > _offset.Low;

        /// <summary>
        /// Checks that end of the partition is achieved by consumer.
        /// </summary>
        /// <typeparam name="K">Message key.</typeparam>
        /// <typeparam name="V">Message value.</typeparam>
        /// <param name="consumeResult">Consumer result.</param>
        public bool IsWatermarkAchievedBy<K, V>(ConsumeResult<K, V> consumeResult)
        {
            if (consumeResult is null)
            {
                throw new ArgumentNullException(nameof(consumeResult));
            }

            return consumeResult.Offset != _offset.High - 1;
        }

        /// <summary>
        /// Assing consumer to a partition as topic.
        /// </summary>
        /// <typeparam name="K">Message key.</typeparam>
        /// <typeparam name="V">Message value.</typeparam>
        /// <param name="consumer">Consumer.</param>
        public void AssingWithConsumer<K, V>(IConsumer<K, V> consumer)
        {
            if (consumer is null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            consumer.Assign(new TopicPartition(_topicName.Value, _partition));

        }

        /// <summary>
        ///  Assing consumer to a partition as topic with offset started from <paramref name="startDate"/>.
        /// </summary>
        /// <typeparam name="K">Message key.</typeparam>
        /// <typeparam name="V">Message value.</typeparam>
        /// <param name="consumer">Consumer.</param>
        /// <param name="startDate">Start date for offset</param>
        /// <param name="timeout">Timeout for offset searching</param>
        public void AssingWithConsumer<K, V>(IConsumer<K, V> consumer, DateTime startDate, TimeSpan timeout)
        {
            if (consumer is null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            var topicPartition = new TopicPartition(_topicName.Value, _partition);

            var partitionTimestamp = new TopicPartitionTimestamp(topicPartition, new Timestamp(startDate));

            var offsets = consumer.OffsetsForTimes(new[] { partitionTimestamp }, timeout);

            var singleOffset = offsets.Single();

            consumer.Assign(singleOffset);
        }

        private readonly Partition _partition;
        private readonly WatermarkOffsets _offset;
        private readonly LoadingTopic _topicName;

    }
}
