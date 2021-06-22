using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaSnapshot.Export.File.Json
{
    /// <summary>
    /// <see cref="JsonFileDataExporter{TKey, TValue, TTopic}"/> for string long key and string json value
    /// </summary>
    public class JsonLongKeyStringValueDataExporter : JsonFileDataExporter<long, string, ExportedFileTopic>
    {
        /// <summary>
        /// Creates <see cref="JsonLongKeyStringValueDataExporter"/>.
        /// </summary>
        /// <param name="logger">Logger for <see cref="JsonLongKeyStringValueDataExporter"/>.</param>
        public JsonLongKeyStringValueDataExporter(ILogger<JsonLongKeyStringValueDataExporter> logger) : base(logger)
        {
        }

        /// <inheritdoc/>
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
