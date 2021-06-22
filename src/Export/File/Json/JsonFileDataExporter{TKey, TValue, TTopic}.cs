using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json.Linq;

using KafkaSnapshot.Abstractions.Export;


namespace KafkaSnapshot.Export.File.Json
{
    public class JsonFileDataExporter<TKey, TValue, TTopic> : IDataExporter<TKey, TValue, TTopic> where TTopic : ExportedFileTopic
    {
        public JsonFileDataExporter(ILogger<JsonFileDataExporter<TKey, TValue, TTopic>> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            _logger.LogDebug("Instance created.");
        }

        public async Task ExportAsync(IDictionary<TKey, TValue> data, TTopic topic, CancellationToken ct)
        {
            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            if (topic is null)
            {
                throw new ArgumentNullException(nameof(topic));
            }

            using var _ = _logger.BeginScope("Data from topic {topic} to File {file} with {items} item(s)", topic.Name, topic.FileName, data.Count);

            _logger.LogDebug("Starting saving data");

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

            _logger.LogDebug("Data saved successfully.");
        }

        private readonly ILogger<JsonFileDataExporter<TKey, TValue, TTopic>> _logger;
    }
}
