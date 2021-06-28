namespace KafkaSnapshot.Abstractions.Filters
{
    /// <summary>
    /// Filter for loading data from Kafka.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public interface IKeyFilter<TKey> where TKey : notnull
    {
        /// <summary>
        /// Check key on matching some creteria.
        /// </summary>
        /// <param name="key">Message key.</param>
        /// <returns>true is match.</returns>
        public bool IsMatch(TKey key);
    }
}
