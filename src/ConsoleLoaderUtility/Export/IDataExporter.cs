using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleLoaderUtility.Export
{
    public interface IDataExporter<T>
    {
        public Task ExportAsync(IEnumerable<T> data, CancellationToken ct);
    }
}
