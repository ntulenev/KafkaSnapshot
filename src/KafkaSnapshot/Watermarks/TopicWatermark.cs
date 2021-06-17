using System;
using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka;

namespace KafkaSnapshot.Watermarks
{
    /// <summary>
    /// Offset watermark for a topic.
    /// </summary>
    public class TopicWatermark
    {
        /// <summary>
        /// Creates watermark for a topic.
        /// </summary>
        /// <param name="partitionWatermarks">Raw topic partition watermarks.</param>
        public TopicWatermark(IEnumerable<PartitionWatermark> partitionWatermarks)
        {
            if (partitionWatermarks is null)
            {
                throw new ArgumentNullException(nameof(partitionWatermarks));
            }
            Watermarks = partitionWatermarks;
        }

        /// <summary>
        /// Topic partition watermarks.
        /// </summary>
        public IEnumerable<PartitionWatermark> Watermarks { get; }

        /// <summary>
        /// Assing consumer for a topic with offset.
        /// </summary>
        /// <typeparam name="K">Message key.</typeparam>
        /// <typeparam name="V">Message value.</typeparam>
        /// <param name="consumer">Consumer.</param>
        public void AssignWithConsumer<K, V>(IConsumer<K, V> consumer)
        {
            if (consumer is null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            consumer.Assign(Watermarks.Select(watermark => watermark.CreateTopicPartitionWithHighOffset()));
        }
    }
}
