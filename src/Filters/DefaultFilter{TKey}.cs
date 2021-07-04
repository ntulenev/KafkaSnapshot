using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters
{
    /// <summary>
    /// Default filter for message (match for any key).
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public class DefaultFilter<TKey> : IKeyFilter<TKey> where TKey : notnull
    {
        /// <summary>
        /// Check if key is match.
        /// </summary>
        /// <param name="key">Message key.</param>
        /// <returns>True for any key.</returns>
        public bool IsMatch(TKey key) => true;
    }
}
