using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.File.Common
{
    /// <summary>
    /// Provider to create filestreams to write data.
    /// </summary>
    public class FileStreamProvider : IFileStreamProvider
    {
        /// <inheritdoc/>
        /// <exception cref="ArgumentException"></exception>
        public Stream CreateFileStream(string fileName)
        {
            ArgumentNullException.ThrowIfNull(fileName);

            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException(
                    "File name cannot be empty or consist of whitespaces.", nameof(fileName));
            }

            return System.IO.File.Create(fileName);
        }
    }
}
