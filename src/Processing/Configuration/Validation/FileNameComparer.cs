using System.Diagnostics.CodeAnalysis;

namespace KafkaSnapshot.Processing.Configuration.Validation
{
    /// <summary>
    /// File name comparer.
    /// </summary>
    public class FileNameComparer : IEqualityComparer<string>
    {
        /// <inheritdoc/>
        public bool Equals(string? x, string? y)
        {
            if (x is not null && y is not null)
            {
                return x.Equals(y, StringComparison.CurrentCultureIgnoreCase);
            }

            return false;
        }

        /// <inheritdoc/>
        public int GetHashCode([DisallowNull] string obj) => HashCode.Combine(obj);
    }
}
