using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Sorting
{
    public class MessageSorter<TKey, TValue> : IMessageSorter<TKey, TValue>
                                             where TKey : notnull
                                             where TValue : notnull
    {
        public IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> Sort(IEnumerable<KeyValuePair<TKey, KafkaMessage<TValue>>> source)
        {
            throw new NotImplementedException();
        }
    }
}
