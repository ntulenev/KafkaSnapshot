using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;

namespace Export.File
{
    public class JsonFileDataExporter<Key, Value> : IDataExporter<Key, Value>
    {
        public async Task ExportAsync(IDictionary<Key, Value> data, string topic, CancellationToken ct)
        {
            var inner = string.Join(",\n", data.Select(x => $"{{ \"key\":{x.Key}, \"value\":{x.Value}}}"));

            var sb = new StringBuilder();
            sb.AppendLine("{");
            sb.AppendLine("\"messages\": [");
            sb.AppendLine(inner);
            sb.AppendLine("]");
            sb.AppendLine("}");

            var path = topic.Replace("-", "_");

            JObject json = JObject.Parse(sb.ToString());

            await System.IO.File.WriteAllTextAsync($"{path}.txt", json.ToString(), ct).ConfigureAwait(false);
        }
    }
}
