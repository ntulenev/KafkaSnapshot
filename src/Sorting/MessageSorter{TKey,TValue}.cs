using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Sorting
{
    /// <summary>
    /// Creates <see cref="MessageSorter{TKey, TValue}"/>.
    /// </summary>
    /// <typeparam name="TKey">Kafka message key type.</typeparam>
    /// <typeparam name="TValue">Kafka message value type.</typeparam>
    public class MessageSorter<TKey, TValue> : IMessageSorter<TKey, TValue>
                                             where TKey : notnull
                                             where TValue : notnull
    {
        /// <summary>
        /// Creates <see cref="MessageSorter{TKey, TValue}"/>.
        /// </summary>
        /// <param name="sortingRules">Sorting parameters.</param>
        /// <exception cref="ArgumentNullException">Throws exception if <paramref name="sortingRules"/> is null.</exception>
        public MessageSorter(SortingParams sortingRules)
        {
            _sortingRules = sortingRules ?? throw new ArgumentNullException(nameof(sortingRules));
        }

        /// <inheritdoc/>
        /// <exception cref="NotImplementedException">If sorting not implemented for this params.</exception>
        public IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> Sort(IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            return (_sortingRules) switch
            {
                { Order: SortingOrder.No, Type: _ } => source,
                { Order: SortingOrder.Ask, Type: SortingType.Time } => source.OrderBy(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Desk, Type: SortingType.Time } => source.OrderByDescending(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Ask, Type: SortingType.Partition } => source.OrderBy(x => x.Value.Meta.Partition)
                                                                                  .ThenBy(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Desk, Type: SortingType.Partition } => source.OrderByDescending(x => x.Value.Meta.Partition)
                                                                                   .ThenBy(x => x.Value.Meta.Timestamp).ToList(),
                _ => throw new NotImplementedException("Sort type not implemented")
            };
        }

        private readonly SortingParams _sortingRules;
    }
}
