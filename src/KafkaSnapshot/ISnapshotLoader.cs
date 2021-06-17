using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaSnapshot
{
    public interface ISnapshotLoader<Key, Message> where Key : notnull
    {
        public Task<IDictionary<Key, Message>> LoadCompactSnapshotAsync(CancellationToken ct);
    }
}
