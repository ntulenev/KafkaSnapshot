using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Abstractions.Filters
{
    /// <summary>
    /// Creates KeyFilter by condition.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public interface IKeyFiltersFactory<TKey> where TKey : notnull
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="sample"></param>
        /// <returns></returns>
        public IKeyFilter<TKey> Create(FilterType type, TKey sample);
    }
}
