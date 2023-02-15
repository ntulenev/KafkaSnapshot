namespace KafkaSnapshot.Abstractions.Processing;

/// <summary>
/// Defines an interface for a processing unit that consumes messages from a Kafka topic.
/// </summary>
public interface IProcessingUnit
{
    /// <summary>
    /// Processes messages from the Kafka topic.
    /// </summary>
    /// <param name="ct">A cancellation token that can be used to stop the processing.</param>
    /// <returns>A task that represents the asynchronous processing.</returns>
    public Task ProcessAsync(CancellationToken ct);

    /// <summary>
    /// Gets the name of the Kafka topic that this processing unit is consuming messages from.
    /// </summary>
    public string TopicName { get; }
}
