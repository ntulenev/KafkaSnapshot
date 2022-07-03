using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters
{
    /// <summary>
    /// Check key on equality with sample.
    /// </summary>
    /// <typeparam name="TKey">Message data type.</typeparam>
    public class EqualsFilter<TData> : IKeyFilter<TData> where TData : notnull
    {
        /// <summary>
        /// Creates <see cref="EqualsFilter{TKey}"/>.
        /// </summary>
        /// <param name="sample">Data sample.</param>
        public EqualsFilter(TData sample)
        {
            _sample = sample ?? throw new ArgumentNullException(nameof(sample));
        }

        /// <inheritdoc/>
        public bool IsMatch(TData key)
        {
            ArgumentNullException.ThrowIfNull(key);

            return key.Equals(_sample);
        }

        private readonly TData _sample;
    }
}
