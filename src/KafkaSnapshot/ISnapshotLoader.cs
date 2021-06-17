using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaSnapshot
{
    /// <summary>
    /// Loader for Kafka topics. Loads topic as Dictionary with compacting per key.
    /// </summary>
    /// <typeparam name="Key">Message key.</typeparam>
    /// <typeparam name="Message">Message value.</typeparam>
    public interface ISnapshotLoader<Key, Message> where Key : notnull
    {
        /// <summary>
        /// Loads topic as Dictionary with compacting per key.
        /// </summary>
        public Task<IDictionary<Key, Message>> LoadCompactSnapshotAsync(CancellationToken ct);
    }
}
