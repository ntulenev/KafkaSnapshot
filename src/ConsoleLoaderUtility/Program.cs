using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Confluent.Kafka;

using Serilog;

using ConsoleLoaderUtility.Tool.Configuration;
using ConsoleLoaderUtility.Tool;

using Export;

using KafkaSnapshot;
using KafkaSnapshot.Metadata;

namespace ConsoleLoaderUtility
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var builder = new HostBuilder()
            .ConfigureAppConfiguration((hostingContext, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: true);
            })
            .ConfigureServices((hostContext, services) =>
            {
                services.AddScoped<LoaderTool>();
                services.AddSingleton<IDataExporter<string, string>, JsonFileDataExporter>();
                services.AddSingleton(sp => CreateTopicLoaders(hostContext.Configuration));
                services.Configure<LoaderToolConfiguration>(hostContext.Configuration.GetSection(nameof(LoaderToolConfiguration)));

                var logger = new LoggerConfiguration()
                                 .ReadFrom.Configuration(hostContext.Configuration)
                                 .CreateLogger();

                services.AddLogging(x =>
                {
                    x.SetMinimumLevel(LogLevel.Information);
                    x.AddSerilog(logger: logger, dispose: true);
                });
            });

            var host = builder.Build();

            using (var serviceScope = host.Services.CreateScope())
            {
                var services = serviceScope.ServiceProvider;

                var tool = services.GetRequiredService<LoaderTool>();
                await tool.ProcessAsync(CancellationToken.None);
            }
        }

        private static IDictionary<string, ISnapshotLoader<string, string>> CreateTopicLoaders(IConfiguration configuration)
        {
            var dictionary = new Dictionary<string, ISnapshotLoader<string, string>>();

            var section = configuration.GetSection(nameof(LoaderToolConfiguration));

            var config = section.Get<LoaderToolConfiguration>();

            var servers = string.Join(",", config.BootstrapServers);

            IConsumer<string, string> createConsumer()
            {
                var conf = new ConsumerConfig
                {
                    BootstrapServers = servers,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    GroupId = Guid.NewGuid().ToString(),
                };

                return new ConsumerBuilder<string, string>(conf).Build();
            }

            foreach (var topic in config.Topics)
            {
                var adminConfig = new AdminClientConfig()
                {
                    BootstrapServers = servers
                };

                var adminClient = new AdminClientBuilder(adminConfig).Build();
                var wLoader = new TopicWatermarkLoader(new TopicName(topic), adminClient, config.MetadataTimeout);
                dictionary.Add(topic, new SnapshotLoader<string, string>(createConsumer, wLoader));
            }

            return dictionary;
        }
    }
}
