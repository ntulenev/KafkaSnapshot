namespace KafkaSnapshot.Abstractions.Export
{
    /// <summary>
    /// Utility that saves string content to file.
    /// </summary>
    public interface IFileSaver
    {
        /// <summary>
        /// Saves string content to file.
        /// </summary>
        /// <param name="fileName">Name of file.</param>
        /// <param name="content">File content.</param>
        /// <param name="ct">Token for cancellation.</param>
        public Task SaveAsync(string fileName, string content, CancellationToken ct);
    }
}
