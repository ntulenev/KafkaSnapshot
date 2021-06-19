using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Export
{
    public interface IDataExporter<Key, Message, TTopic> where TTopic : ExportedTopic
    {
        public Task ExportAsync(IDictionary<Key, Message> data, TTopic topic, CancellationToken ct);
    }
}
