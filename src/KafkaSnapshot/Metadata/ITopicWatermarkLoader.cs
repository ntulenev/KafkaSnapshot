using System;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using KafkaSnapshot.Watermarks;

namespace KafkaSnapshot.Metadata
{
    /// <summary>
    /// Interface for service that loads <see cref="TopicWatermark"/>.
    /// </summary>
    public interface ITopicWatermarkLoader
    {
        /// <summary>
        /// Loads <see cref="TopicWatermark"/> from Kafka.
        /// </summary>
        /// <typeparam name="Key">Message key.</typeparam>
        /// <typeparam name="Value">Message value.</typeparam>
        /// <param name="consumerFactory">Factory delegate for creating consumer.</param>
        /// <param name="ct">Cancellation token.</param>
        public Task<TopicWatermark> LoadWatermarksAsync<Key, Value>(Func<IConsumer<Key, Value>> consumerFactory, CancellationToken ct);
    }
}
