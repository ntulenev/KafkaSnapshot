using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleLoaderUtility.Export
{
    public interface IDataExporter<Key, Message>
    {
        public Task ExportAsync(IDictionary<Key, Message> data, CancellationToken ct);
    }
}
