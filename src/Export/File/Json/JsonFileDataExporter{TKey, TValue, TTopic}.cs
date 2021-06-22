using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

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

            await System.IO.File.WriteAllTextAsync($"{topic.FileName}", PrepareJson(data), ct).ConfigureAwait(false);

            _logger.LogDebug("Data saved successfully.");
        }

        protected virtual string PrepareJson(IDictionary<TKey, TValue> data) => JsonConvert.SerializeObject(data, Formatting.Indented);

        private readonly ILogger<JsonFileDataExporter<TKey, TValue, TTopic>> _logger;
    }
}
