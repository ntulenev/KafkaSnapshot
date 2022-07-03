using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters
{
    /// <summary>
    /// Default filter for message (match for any data).
    /// </summary>
    /// <typeparam name="TData">Data type.</typeparam>
    public class DefaultFilter<TData> : IKeyFilter<TData> where TData : notnull
    {
        /// <summary>
        /// Check if key is match.
        /// </summary>
        /// <param name="key">Message key.</param>
        /// <returns>True for any key.</returns>
        public bool IsMatch(TData key) => true;
    }
}
