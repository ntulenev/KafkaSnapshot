using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Abstractions.Export
{
    public interface IDataExporter<TKey, TMessage, TTopic> where TTopic : ExportedTopic
    {
        public Task ExportAsync(IDictionary<TKey, TMessage> data, TTopic topic, CancellationToken ct);
    }
}
