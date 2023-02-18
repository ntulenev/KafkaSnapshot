using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Names;

namespace KafkaSnapshot.Export.File.Common
{
    /// <summary>
    /// Provider to create filestreams to write data.
    /// </summary>
    public class FileStreamProvider : IFileStreamProvider
    {
        /// <inheritdoc/>
        /// <exception cref="ArgumentNullException">Thrown when fileName is null.</exception>
        /// <exception cref="ArgumentException">Thrown when fileName is empty or consists of whitespaces.</exception>
        public Stream CreateFileStream(FileName fileName)
        {
            ArgumentNullException.ThrowIfNull(fileName);

            return System.IO.File.Create(fileName.FullName);
        }
    }
}
