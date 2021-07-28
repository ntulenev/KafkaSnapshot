using System;

namespace KafkaSnapshot.Models.Message
{
    /// <summary>
    /// Message with timestamp
    /// </summary>
    /// <typeparam name="TMessage">Message type</typeparam>
    public record DatedMessage<TMessage>(TMessage Message, DateTime Timestamp)
        where TMessage : notnull;
}
