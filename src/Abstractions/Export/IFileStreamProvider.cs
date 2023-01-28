namespace KafkaSnapshot.Abstractions.Export;

/// <summary>
/// Utility that provedes file stream for saving data.
/// </summary>
public interface IFileStreamProvider
{
    /// <summary>
    /// Creates stream for saving data.
    /// </summary>
    /// <param name="fileName">Name of file.</param>
    /// <returns>File stream for saving data.</returns>
    public Stream CreateFileStream(string fileName);
}
