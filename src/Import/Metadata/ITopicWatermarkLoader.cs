using Confluent.Kafka;

using KafkaSnapshot.Import.Watermarks;
using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Import.Metadata;

/// <summary>
/// Interface for service that loads <see cref="TopicWatermark"/>.
/// </summary>
public interface ITopicWatermarkLoader
{
    /// <summary>
    /// Loads <see cref="TopicWatermark"/> from Kafka.
    /// </summary>
    /// <typeparam name="TKey">Message key.</typeparam>
    /// <typeparam name="TValue">Message value.</typeparam>
    /// <param name="consumerFactory">Factory delegate for creating consumer.</param>
    /// <param name="loadingTopic">Kafka topic data.</param>
    /// <param name="ct">Cancellation token.</param>
    public Task<TopicWatermark> LoadWatermarksAsync<TKey, TValue>(
        Func<IConsumer<TKey, TValue>> consumerFactory,
        LoadingTopic loadingTopic,
        CancellationToken ct);
}
