namespace KafkaSnapshot.Abstractions.Export;

/// <summary>
/// Interface for a file stream provider.
/// </summary>
public interface IFileStreamProvider
{
    /// <summary>
    /// Creates a file stream for a given file name.
    /// </summary>
    /// <param name="fileName">The name of the file to create a stream for.</param>
    /// <returns>A stream object representing the created file.</returns>
    public Stream CreateFileStream(string fileName);
}
