using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters
{
    /// <summary>
    /// Check key on equality with sample.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public class EqualsFilter<TKey> : IKeyFilter<TKey> where TKey : notnull
    {
        /// <summary>
        /// Creates <see cref="EqualsFilter{TKey}"/>.
        /// </summary>
        /// <param name="sample">Key sample.</param>
        public EqualsFilter(TKey sample)
        {
            _sample = sample ?? throw new ArgumentNullException(nameof(sample));
        }

        /// <inheritdoc/>
        public bool IsMatch(TKey key)
        {
            if (key is null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return key.Equals(_sample);
        }

        private readonly TKey _sample;
    }
}
