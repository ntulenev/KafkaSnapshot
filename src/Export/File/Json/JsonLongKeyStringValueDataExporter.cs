using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Abstractions.Export;

namespace KafkaSnapshot.Export.File.Json
{
    /// <summary>
    /// <see cref="JsonFileDataExporter{TKey, TValue, TTopic}"/> for string long key and string json value
    /// </summary>
    public class JsonLongKeyStringValueDataExporter : JsonFileDataExporter<long, string, ExportedTopic>
    {
        /// <summary>
        /// Creates <see cref="JsonLongKeyStringValueDataExporter"/>.
        /// </summary>
        /// <param name="logger">Logger for <see cref="JsonLongKeyStringValueDataExporter"/>.</param>
        /// <param name="fileSaver">Utility that saves content to file.</param>
        public JsonLongKeyStringValueDataExporter(ILogger<JsonLongKeyStringValueDataExporter> logger, IFileSaver fileSaver) : base(logger, fileSaver)
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
