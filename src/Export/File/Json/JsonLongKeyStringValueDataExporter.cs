using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaSnapshot.Export.File.Json
{
    public class JsonLongKeyStringValueDataExporter : JsonFileDataExporter<long, string, ExportedFileTopic>
    {
        public JsonLongKeyStringValueDataExporter(ILogger<JsonLongKeyStringValueDataExporter> logger) : base(logger)
        {
        }

        protected override string PrepareJson(IDictionary<long, string> data)
        {
            var items = data.Select(x => new
            {
                x.Key,
                Value = JToken.Parse(x.Value)
            });
            return JsonConvert.SerializeObject(items, Formatting.Indented);
        }
    }
}
