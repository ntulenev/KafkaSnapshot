using System;
using System.Threading;
using System.Threading.Tasks;

using Export;

using KafkaSnapshot;


namespace ConsoleLoaderUtility.Tool
{
    class ProcessingUnit<Key, Value> : IProcessingUnit where Key : notnull
    {
        public ProcessingUnit(string topic, ISnapshotLoader<Key, Value> processor, IDataExporter<Key, Value> exporter)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentNullException(nameof(topic));
            }

            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Topic is empty or contains only whitespaces", nameof(topic));
            }

            _topic = topic;
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
        }

        public async Task ProcessAsync(CancellationToken ct)
        {

            var items = await _processor.LoadCompactSnapshotAsync(ct);
            await _exporter.ExportAsync(items, _topic, ct);
        }

        public string Topic => _topic;

        private readonly ISnapshotLoader<Key, Value> _processor;
        private readonly IDataExporter<Key, Value> _exporter;
        private readonly string _topic;
    }
}
