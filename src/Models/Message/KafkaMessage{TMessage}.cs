namespace KafkaSnapshot.Models.Message;

/// <summary>
/// Kafka message with metadata.
/// </summary>
/// <typeparam name="TMessage">Message type.</typeparam>
public sealed record KafkaMessage<TMessage>
    where TMessage : notnull
{
    /// <summary>
    /// Creates <see cref="KafkaMessage{TMessage}"/>.
    /// </summary>
    /// <param name="message">Kafka message.</param>
    /// <param name="meta">Message metadata.</param>
    public KafkaMessage(TMessage message, KafkaMetadata meta)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(meta);

        Message = message;
        Meta = meta;
    }

    /// <summary>
    /// Kafka message.
    /// </summary>
    public TMessage Message { get; init; }

    /// <summary>
    /// Message metadata.
    /// </summary>
    public KafkaMetadata Meta { get; init; }
}
