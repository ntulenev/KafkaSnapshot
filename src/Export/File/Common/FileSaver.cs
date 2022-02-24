using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.File.Common
{
    /// <inheritdoc/>
    public class FileSaver : IFileSaver
    {
        /// <inheritdoc/>
        public async Task SaveAsync(string fileName, string content, CancellationToken ct)
        {
            ArgumentNullException.ThrowIfNull(fileName);

            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException(
                    "File name cannot be empty or consist of whitespaces.", nameof(fileName));
            }

            ArgumentNullException.ThrowIfNull(content);

            await System.IO.File.WriteAllTextAsync(fileName, content, ct).ConfigureAwait(false);
        }
    }
}
