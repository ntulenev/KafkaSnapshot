using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Newtonsoft.Json.Linq;

using KafkaSnapshot.Abstractions.Export;

namespace Export.File
{
    public class JsonFileDataExporter<TKey, TValue, TTopic> : IDataExporter<TKey, TValue, TTopic> where TTopic : ExportedFileTopic
    {
        public async Task ExportAsync(IDictionary<TKey, TValue> data, TTopic topic, CancellationToken ct)
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
