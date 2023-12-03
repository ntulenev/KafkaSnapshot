namespace KafkaSnapshot.Import.Configuration;

/// <summary>
/// Kafka Admin Client Configuration.
/// </summary>
public class TopicWatermarkLoaderConfiguration
{
    /// <summary>
    /// Timout for metadata request.
    /// </summary>
    public TimeSpan AdminClientTimeout { get; init; }
}
