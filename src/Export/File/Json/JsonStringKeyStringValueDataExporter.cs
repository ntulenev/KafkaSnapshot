using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaSnapshot.Export.File.Json
{
    public class JsonStringKeyStringValueDataExporter : JsonFileDataExporter<string, string, ExportedFileTopic>
    {
        public JsonStringKeyStringValueDataExporter(ILogger<JsonStringKeyStringValueDataExporter> logger) : base(logger)
        {
        }

        protected override string PrepareJson(IDictionary<string, string> data)
        {
            var items = data.Select(x => new
            {
                Key = JToken.Parse(x.Key),
                Value = JToken.Parse(x.Value)
            });
            return JsonConvert.SerializeObject(items, Formatting.Indented);
        }
    }
}
