using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Message;
using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Sorting
{
    public class MessageSorter<TKey, TValue> : IMessageSorter<TKey, TValue>
                                             where TKey : notnull
                                             where TValue : notnull
    {
        public MessageSorter(SortingParams sortingRules)
        {
            _sortingRules = sortingRules ?? throw new ArgumentNullException(nameof(sortingRules));
        }

        public IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> Sort(IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> source)
        {
            return (_sortingRules) switch
            {
                { Order: SortingOrder.No, Type: _ } => source,
                { Order: SortingOrder.Ask, Type: SortingType.Time } => source.OrderBy(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Desk, Type: SortingType.Time } => source.OrderByDescending(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Ask, Type: SortingType.Partition } => source.OrderBy(x => x.Value.Meta.Partition).ThenBy(x => x.Value.Meta.Timestamp).ToList(),
                { Order: SortingOrder.Desk, Type: SortingType.Partition } => source.OrderByDescending(x => x.Value.Meta.Partition)
                                                                                   .ThenBy(x => x.Value.Meta.Timestamp).ToList(),
                _ => throw new NotImplementedException("Sort type not implemented")
            };
        }

        private readonly SortingParams _sortingRules;
    }
}
