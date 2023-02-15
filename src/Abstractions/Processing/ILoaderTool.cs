namespace KafkaSnapshot.Abstractions.Processing;

//// <summary>
/// Interface for a loader tool that performs processing main app logic.
/// </summary>
public interface ILoaderTool
{
    /// <summary>
    /// Runs processing Kafka topics.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Task representing the asynchronous operation.</returns>
    public Task ProcessAsync(CancellationToken ct);
}
