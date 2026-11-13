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
    /// <param name="loadingTopic">Kafka topic data.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<TopicWatermark> LoadWatermarksAsync(
        LoadingTopic loadingTopic,
        CancellationToken ct);
}
