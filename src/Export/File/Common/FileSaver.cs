using System;
using System.Threading;
using System.Threading.Tasks;

using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.File.Common
{
    /// <inheritdoc/>
    public class FileSaver : IFileSaver
    {
        /// <inheritdoc/>
        public async Task SaveAsync(string fileName, string content, CancellationToken ct)
        {
            if (fileName is null)
            {
                throw new ArgumentNullException(nameof(fileName));
            }

            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException(
                    "File name cannot be empty or consist of whitespaces.", nameof(fileName));
            }

            if (content is null)
            {
                throw new ArgumentNullException(nameof(content));
            }

            await System.IO.File.WriteAllTextAsync(fileName, content, ct).ConfigureAwait(false);
        }
    }
}
