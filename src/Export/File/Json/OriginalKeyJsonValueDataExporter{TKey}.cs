using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Markers;

namespace KafkaSnapshot.Export.File.Json
{
    /// <summary>
    /// <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/> for key <see cref="TKey"/> and string json value.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    public class OriginalKeyJsonValueDataExporter<TKey> : JsonFileDataExporter<TKey, OriginalKeyMarker, string, ExportedTopic>
    {
        /// <summary>
        /// Creates <see cref="OriginalKeyJsonValueDataExporter"/>.
        /// </summary>
        /// <param name="logger">Logger for <see cref="OriginalKeyJsonValueDataExporter{TKey}"/>.</param>
        /// <param name="fileSaver">Utility that saves content to file.</param>
        public OriginalKeyJsonValueDataExporter(ILogger<OriginalKeyJsonValueDataExporter<TKey>> logger, IFileSaver fileSaver) : base(logger, fileSaver)
        {
        }

        /// <inheritdoc/>
        protected override string PrepareJson(IEnumerable<KeyValuePair<TKey, string>> data)
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
