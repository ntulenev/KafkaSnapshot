namespace KafkaSnapshot.Models.Message
{
    /// <summary>
    /// Message with timestamp
    /// </summary>
    /// <typeparam name="TMessage">Message type</typeparam>
    /// <param name="Message">Kafka message.</param>
    /// <param name="Timestamp">Message timestamp.</param>
    public record DatedMessage<TMessage>(TMessage Message, MessageMeta Meta)
        where TMessage : notnull;
}
