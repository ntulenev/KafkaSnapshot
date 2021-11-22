namespace KafkaSnapshot.Abstractions.Processing
{
    /// <summary>
    /// Runner for processing units.
    /// </summary>
    public interface ILoaderTool
    {
        /// <summary>
        /// Runs processing units.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        public Task ProcessAsync(CancellationToken ct);
    }
}
