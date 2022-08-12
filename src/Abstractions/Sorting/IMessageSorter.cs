using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Sorting
{
    public interface IMessageSorter<TKey, TValue> where TKey : notnull
                                                  where TValue : notnull
    {
        public IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> Sort(IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> source);
    }
}
