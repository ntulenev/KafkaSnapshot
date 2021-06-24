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
    /// <see cref="JsonFileDataExporter{TKey, TValue, TTopic}"/> for string json key and string json value.
    /// </summary>
    public class JsonStringKeyStringValueDataExporter : JsonFileDataExporter<string, string, ExportedTopic>
    {
        /// <summary>
        /// Creates <see cref="JsonStringKeyStringValueDataExporter"/>.
        /// </summary>
        /// <param name="logger">Logger for <see cref="JsonStringKeyStringValueDataExporter"/>.</param>
        /// <param name="fileSaver">Utility that saves content to file.</param>
        public JsonStringKeyStringValueDataExporter(ILogger<JsonStringKeyStringValueDataExporter> logger, IFileSaver fileSaver) : base(logger, fileSaver)
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
