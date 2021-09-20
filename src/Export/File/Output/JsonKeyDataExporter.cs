using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.File.Output
{
    /// <summary>
    /// <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/> for string json key and string json value.
    /// </summary>
    public class JsonKeyDataExporter : JsonFileDataExporter<string, JsonKeyMarker, string, ExportedTopic>
    {
        /// <summary>
        /// Creates <see cref="JsonKeyDataExporter"/>.
        /// </summary>
        /// <param name="logger">Logger for <see cref="JsonKeyDataExporter"/>.</param>
        /// <param name="fileSaver">Utility that saves content to file.</param>
        public JsonKeyDataExporter(ILogger<JsonKeyDataExporter> logger, IFileSaver fileSaver) : base(logger, fileSaver)
        {
        }

        /// <inheritdoc/>
        protected override string PrepareJson(IEnumerable<KeyValuePair<string, DatedMessage<string>>> data, bool exportRawMessage)
        {
            var items = data.Select(x => new
            {
                Key = JToken.Parse(x.Key),
                Value = exportRawMessage ? x.Value.Message : JToken.Parse(x.Value.Message),
                x.Value.Timestamp
            });

            return JsonConvert.SerializeObject(items, Formatting.Indented);
        }
    }
}
