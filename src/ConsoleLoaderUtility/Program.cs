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
using Export.File;

namespace ConsoleLoaderUtility
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var builder = new HostBuilder()
                    .ConfigureAppConfiguration((hostingContext, config) =>
                    {
                        config.AddJsonFile("appsettings.json", optional: true);
                    })
                    .ConfigureServices((hostContext, services) =>
                    {
                        services.AddScoped(typeof(LoaderTool));
                        services.AddSingleton(typeof(IDataExporter<,>), typeof(JsonFileDataExporter<,>));
                        services.AddSingleton(sp => CreateTopicLoaders(sp, hostContext.Configuration));
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
            catch (Exception ex)
            {
                Console.WriteLine($"Error {ex}");
            }
        }

        private static ICollection<IProcessingUnit> CreateTopicLoaders(IServiceProvider sp, IConfiguration configuration)
        {
            var list = new List<IProcessingUnit>();

            var section = configuration.GetSection(nameof(LoaderToolConfiguration));

            var config = section.Get<LoaderToolConfiguration>();

            // TODO Add config validation

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

                // TODO Add support different Key type + Add custom deserializer if any needed
                list.Add(new ProcessingUnit<string, string>(topic,
                                            new SnapshotLoader<string, string>(createConsumer, wLoader),
                                            sp.GetRequiredService<IDataExporter<string, string>>()
                                            )
                        );
            }

            return list;
        }
    }
}
