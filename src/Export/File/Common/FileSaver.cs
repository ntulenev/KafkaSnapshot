using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Export.File.Common;

/// <inheritdoc/>
public class FileSaver : IFileSaver
{
    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException">Thrown if fileName 
    /// or content is null.</exception>
    /// <exception cref="ArgumentException">Thrown if fileName is 
    /// empty or consists of whitespaces.</exception>
    public async Task SaveAsync(FileName fileName, string content, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(content);

        await System.IO.File.WriteAllTextAsync(fileName.FullName, content, ct)
                            .ConfigureAwait(false);
    }
}
