namespace KafkaSnapshot.Models.Names;

/// <summary>
/// Represents a file name with its extension.
/// </summary>
public class FileName
{
    /// <summary>
    /// Gets the full name of the file, including the extension.
    /// </summary>
    public string FullName => _fullName;

    /// <summary>
    /// Gets the extension of the file, including the dot.
    /// </summary>
    public string Extension => _extension;

    /// <summary>
    /// Initializes a new instance of the <see cref="FileName"/> 
    /// class with the specified file name.
    /// </summary>
    /// <param name="fileName">The file name, including the extension.</param>
    /// <exception cref="ArgumentNullException">Thrown 
    /// if <paramref name="fileName"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown 
    /// if <paramref name="fileName"/> is null, empty, or whitespace.</exception>
    public FileName(string fileName)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        if (string.IsNullOrWhiteSpace(fileName))
        {
            throw new ArgumentException(
                        "File name cannot be null, empty or whitespace.", 
                        nameof(fileName));
        }

        _fullName = fileName;
        _extension = Path.GetExtension(fileName);
    }

    private readonly string _fullName;
    private readonly string _extension;
}
