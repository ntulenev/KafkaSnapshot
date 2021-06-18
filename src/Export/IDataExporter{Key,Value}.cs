using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Export
{
    public interface IDataExporter<Key, Message>
    {
        public Task ExportAsync(IDictionary<Key, Message> data, string topic, CancellationToken ct);
    }
}
