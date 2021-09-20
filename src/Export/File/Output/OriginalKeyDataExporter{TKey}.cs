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
    /// <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/> for key <see cref="TKey"/> and string json value.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public class OriginalKeyDataExporter<TKey> : JsonFileDataExporter<TKey, OriginalKeyMarker, string, ExportedTopic>
    {
        /// <summary>
        /// Creates <see cref="OriginalKeyJsonValueDataExporter"/>.
        /// </summary>
        /// <param name="logger">Logger for <see cref="OriginalKeyDataExporter{TKey}"/>.</param>
        /// <param name="fileSaver">Utility that saves content to file.</param>
        public OriginalKeyDataExporter(ILogger<OriginalKeyDataExporter<TKey>> logger, IFileSaver fileSaver) : base(logger, fileSaver)
        {
        }

        /// <inheritdoc/>
        protected override string PrepareJson(IEnumerable<KeyValuePair<TKey, DatedMessage<string>>> data, bool exportRawMessage)
        {
            var items = data.Select(x => new
            {
                x.Key,
                Value = exportRawMessage ? x.Value.Message : JToken.Parse(x.Value.Message),
                x.Value.Timestamp
            });

            return JsonConvert.SerializeObject(items, Formatting.Indented);
        }
    }
}
