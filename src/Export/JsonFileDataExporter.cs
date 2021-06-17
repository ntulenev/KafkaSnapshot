using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Export
{
    public class JsonFileDataExporter : IDataExporter<string, string>
    {
        public async Task ExportAsync(IDictionary<string, string> data, string topic, CancellationToken ct)
        {
            var inner = string.Join(",\n", data.Values);
            var sb = new StringBuilder();
            sb.AppendLine("{");
            sb.AppendLine("\"items\": [");
            sb.AppendLine(inner);
            sb.AppendLine("]");
            sb.AppendLine("}");
            var path = topic.Replace("-", "_");
            await File.WriteAllTextAsync($"{path}.txt", sb.ToString(), ct).ConfigureAwait(false);
        }
    }
}
