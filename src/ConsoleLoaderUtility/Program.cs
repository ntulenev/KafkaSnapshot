using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Confluent.Kafka;

using KafkaSnapshot;
using KafkaSnapshot.Metadata;

using ConsoleLoaderUtility.Export;

namespace ConsoleLoaderUtility
{
    class Program
    {
        static async Task Main(string[] args)
        {
            int indexer = 0;

            foreach (var topic in _topics)
            {
                Console.WriteLine($"{++indexer}/{_topics.Count} Processing topic {topic}.");

                Func<IConsumer<string, string>> cFactory = () =>
                {
                    var conf = new ConsumerConfig
                    {
                        BootstrapServers = GetServers(),
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        GroupId = Guid.NewGuid().ToString()
                    };

                    return new ConsumerBuilder<string, string>(conf).Build();
                };

                var timeout = 1000;

                var adminConfig = new AdminClientConfig()
                {
                    BootstrapServers = GetServers()
                };

                var adminClient = new AdminClientBuilder(adminConfig).Build();

                var wLoader = new TopicWatermarkLoader(new TopicName(topic), adminClient, timeout);

                var exporter = new JsonFileDataExporter($"{topic.Replace('-', '_')}.txt");
                var loader = new SnapshotLoader<string, string>(cFactory, wLoader);
                var dump = await loader.LoadCompactSnapshotAsync(CancellationToken.None);
                await exporter.ExportAsync(dump, CancellationToken.None);
            }

            Console.WriteLine("Done.");
        }

        private static string GetServers() => string.Join(",", _bootstrapServers);

        private static readonly List<string> _bootstrapServers = new List<string>()
        {

        };

        private static readonly List<string> _topics = new List<string>
        {

        };
    }
}
