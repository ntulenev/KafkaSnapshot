using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Abstractions.Import
{
    /// <summary>
    /// Loader for Kafka topics. Loads topic as Dictionary with compacting per key.
    /// </summary>
    /// <typeparam name="Key">Message key.</typeparam>
    /// <typeparam name="Message">Message value.</typeparam>
    public interface ISnapshotLoader<TKey, TMessage> where TKey : notnull
                                                     where TMessage : notnull
    {
        /// <summary>
        /// Loads topic as Dictionary with compacting per key.
        /// </summary>
        public Task<IEnumerable<KeyValuePair<TKey, DatedMessage<TMessage>>>> LoadCompactSnapshotAsync(
            LoadingTopic topicName,
            IKeyFilter<TKey> filter,
            CancellationToken ct);
    }
}
