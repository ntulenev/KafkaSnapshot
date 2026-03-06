namespace KafkaSnapshot.Models.Filters;

/// <summary>
/// Keys types of Kafka message.
/// </summary>
#pragma warning disable CA1720
public enum KeyType
{
    /// <summary>
    /// Key is encoded as JSON.
    /// </summary>
    Json,

    /// <summary>
    /// Key is encoded as plain text.
    /// </summary>
    String,

    /// <summary>
    /// Key is encoded as 64-bit integer.
    /// </summary>
    Long,

    /// <summary>
    /// Key is ignored.
    /// </summary>
    Ignored
}
#pragma warning restore CA1720
