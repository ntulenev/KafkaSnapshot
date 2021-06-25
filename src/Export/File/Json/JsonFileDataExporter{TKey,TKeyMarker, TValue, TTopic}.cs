using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Models.Export;

namespace KafkaSnapshot.Export.File.Json
{
    /// <summary>
    /// Default Json File exporter.
    /// </summary>
    /// <typeparam name="TKey">Message Key.</typeparam>
    /// <typeparam name="TKeyMarker">Key marker.</typeparam>
    /// <typeparam name="TValue">Message Value.</typeparam>
    /// <typeparam name="TTopic">Topic object.</typeparam>
    public class JsonFileDataExporter<TKey, TKeyMarker, TValue, TTopic> : IDataExporter<TKey, TKeyMarker, TValue, TTopic>
                        where TTopic : ExportedTopic
                        where TKeyMarker : IKeyRepresentationMarker
    {
        /// <summary>
        /// Creates <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/>.
        /// </summary>
        /// <param name="logger">Logger for <see cref="JsonFileDataExporter{TKey, TKeyMarker, TValue, TTopic}"/>.</param>
        /// <param name="fileSaver">Utility that saves content to file.</param>
        public JsonFileDataExporter(ILogger<JsonFileDataExporter<TKey, TKeyMarker, TValue, TTopic>> logger, IFileSaver fileSaver)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _fileSaver = fileSaver ?? throw new ArgumentNullException(nameof(fileSaver));

            _logger.LogDebug("Instance created.");
        }

        /// <inheritdoc/>
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

            using var _ = _logger.BeginScope("Data from topic {topic} to File {file} with {items} item(s)", topic.Name, topic.ExportName, data.Count);

            _logger.LogDebug("Starting saving data");

            await _fileSaver.SaveAsync($"{topic.ExportName}", PrepareJson(data), ct).ConfigureAwait(false);

            _logger.LogDebug("Data saved successfully.");
        }

        /// <summary>
        /// Converts data as json string.
        /// </summary>
        /// <param name="data">input data.</param>
        /// <returns>Json string.</returns>
        protected virtual string PrepareJson(IDictionary<TKey, TValue> data) => JsonConvert.SerializeObject(data, Formatting.Indented);

        private readonly ILogger<JsonFileDataExporter<TKey, TKeyMarker, TValue, TTopic>> _logger;
        private readonly IFileSaver _fileSaver;
    }
}
