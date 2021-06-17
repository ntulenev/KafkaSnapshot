using System;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Options;

using Confluent.Kafka;

using ConsoleLoaderUtility.Export;
using ConsoleLoaderUtility.Tool.Configuration;

using KafkaSnapshot;
using KafkaSnapshot.Metadata;

namespace ConsoleLoaderUtility.Tool
{
    public class LoaderTool
    {
        public LoaderTool(IOptions<LoaderToolConfiguration> options)
        {
            if (options is null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            if (options.Value is null)
            {
                throw new ArgumentException("Options is not set", nameof(options));
            }

            _options = options.Value;
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            int indexer = 0;

            var servers = string.Join(",", _options.BootstrapServers);

            foreach (var topic in _options.Topics)
            {
                Console.WriteLine($"{++indexer}/{_options.Topics.Count} Processing topic {topic}.");

                Func<IConsumer<string, string>> cFactory = () =>
                {
                    var conf = new ConsumerConfig
                    {
                        BootstrapServers = servers,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        GroupId = Guid.NewGuid().ToString()
                    };

                    return new ConsumerBuilder<string, string>(conf).Build();
                };

                var adminConfig = new AdminClientConfig()
                {
                    BootstrapServers = servers
                };

                var adminClient = new AdminClientBuilder(adminConfig).Build();

                var wLoader = new TopicWatermarkLoader(new TopicName(topic), adminClient, _options.MetadataTimeout);

                var exporter = new JsonFileDataExporter($"{topic.Replace('-', '_')}.txt");
                var loader = new SnapshotLoader<string, string>(cFactory, wLoader);
                var dump = await loader.LoadCompactSnapshotAsync(CancellationToken.None);
                await exporter.ExportAsync(dump, CancellationToken.None);
            }

            Console.WriteLine("Done.");
        }

        private readonly LoaderToolConfiguration _options;
    }
}
