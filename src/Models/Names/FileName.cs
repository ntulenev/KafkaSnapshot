namespace KafkaSnapshot.Models.Names;

/// <summary>
/// Represents a file name with its extension.
/// </summary>
public sealed class FileName
{
    /// <summary>
    /// Gets the full name of the file, including the extension.
    /// </summary>
    public string FullName { get; }

    /// <summary>
    /// Gets the extension of the file, including the dot.
    /// </summary>
    public string Extension { get; }

    /// <summary>Resolves the destination against the configured output directory.</summary>
    public string ResolvePath(string? outputDirectory)
        => Path.GetFullPath(Path.Combine(outputDirectory ?? string.Empty, FullName));

    /// <summary>
    /// Initializes a new instance of the <see cref="FileName"/> 
    /// class with the specified file name.
    /// </summary>
    /// <param name="fileName">The file name, including the extension.</param>
    /// <exception cref="ArgumentNullException">Thrown 
    /// if <paramref name="fileName"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown 
    /// if <paramref name="fileName"/> is empty or whitespace.</exception>
    public FileName(string fileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);

        FullName = fileName;
        Extension = Path.GetExtension(fileName);
    }
}
