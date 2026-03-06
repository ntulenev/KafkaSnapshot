namespace KafkaSnapshot.Models.Filters;

/// <summary>
/// Value types of Kafka message.
/// </summary>
public enum ValueMessageType
{
    /// <summary>
    /// Value is treated as raw bytes/text.
    /// </summary>
    Raw,

    /// <summary>
    /// Value is treated as JSON.
    /// </summary>
    Json
}
