using System;
using System.Threading;
using System.Threading.Tasks;

using ConsoleLoaderUtility.Tool.Configuration;

using Export;

using KafkaSnapshot;


namespace ConsoleLoaderUtility.Tool
{
    class ProcessingUnit<Key, Value> : IProcessingUnit where Key : notnull
    {
        public ProcessingUnit(LoadedTopic topic, ISnapshotLoader<Key, Value> processor, IDataExporter<Key, Value, ExportedFileTopic> exporter)
        {
            _topic = topic ?? throw new ArgumentNullException(nameof(topic));
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
        }

        public async Task ProcessAsync(CancellationToken ct)
        {

            var items = await _processor.LoadCompactSnapshotAsync(ct);
            await _exporter.ExportAsync(items, new ExportedFileTopic(_topic.Name, _topic.ExportFileName), ct);
        }

        public LoadedTopic Topic => _topic;

        private readonly ISnapshotLoader<Key, Value> _processor;
        private readonly IDataExporter<Key, Value, ExportedFileTopic> _exporter;
        private readonly LoadedTopic _topic;
    }
}
