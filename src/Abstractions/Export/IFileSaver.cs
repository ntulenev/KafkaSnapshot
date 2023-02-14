namespace KafkaSnapshot.Abstractions.Export;


/// <summary>
/// Provides a contract for saving content to a file.
/// </summary>
public interface IFileSaver
{
    /// <summary>
    /// Saves the specified content to the file with the specified name.
    /// </summary>
    /// <param name="fileName">The name of the file to save the content to.</param>
    /// <param name="content">The content to save to the file.</param>
    /// <param name="ct">The cancellation token to cancel the operation if needed.</param>
    /// <returns>A task that represents the asynchronous save operation.</returns>
    public Task SaveAsync(string fileName, string content, CancellationToken ct);
}
