namespace KafkaSnapshot.Models.Message
{
    /// <summary>
    /// Kafka message
    /// </summary>
    /// <typeparam name="TMessage">Message type</typeparam>
    /// <param name="Message">Kafka message.</param>
    /// <param name="Meta">Message metadata.</param>
    public record DatedMessage<TMessage>(TMessage Message, MessageMeta Meta)
        where TMessage : notnull;
}
