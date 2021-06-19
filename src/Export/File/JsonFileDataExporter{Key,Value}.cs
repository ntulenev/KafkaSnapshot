using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;

namespace Export.File
{
    public class JsonFileDataExporter<Key, Value, TTopic> : IDataExporter<Key, Value, TTopic> where TTopic : ExportedFileTopic
    {
        public async Task ExportAsync(IDictionary<Key, Value> data, TTopic topic, CancellationToken ct)
        {
            var inner = string.Join(",\n", data.Select(x => $"{{ \"key\":{x.Key}, \"value\":{x.Value}}}"));

            var sb = new StringBuilder();
            sb.AppendLine("{");
            sb.AppendLine("\"messages\": [");
            sb.AppendLine(inner);
            sb.AppendLine("]");
            sb.AppendLine("}");

            var path = topic.FileName;

            JObject json = JObject.Parse(sb.ToString());

            await System.IO.File.WriteAllTextAsync($"{path}", json.ToString(), ct).ConfigureAwait(false);
        }
    }
}
