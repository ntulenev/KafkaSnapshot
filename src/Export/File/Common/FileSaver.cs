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
            if (string.IsNullOrEmpty(fileName))
            {
                throw new ArgumentNullException(nameof(fileName));
            }

            if (string.IsNullOrEmpty(content))
            {
                throw new ArgumentNullException(nameof(content));
            }

            await System.IO.File.WriteAllTextAsync(fileName, content, ct).ConfigureAwait(false);
        }
    }
}
