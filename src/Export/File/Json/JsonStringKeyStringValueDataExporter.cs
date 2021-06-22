using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaSnapshot.Export.File.Json
{
    /// <summary>
    /// <see cref="JsonFileDataExporter{TKey, TValue, TTopic}"/> for string json key and string json value
    /// </summary>
    public class JsonStringKeyStringValueDataExporter : JsonFileDataExporter<string, string, ExportedFileTopic>
    {
        /// <summary>
        /// Creates <see cref="JsonStringKeyStringValueDataExporter"/>.
        /// </summary>
        /// <param name="logger">Logger for <see cref="JsonStringKeyStringValueDataExporter"/>.</param>
        public JsonStringKeyStringValueDataExporter(ILogger<JsonStringKeyStringValueDataExporter> logger) : base(logger)
        {
        }

        /// <inheritdoc/>
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
