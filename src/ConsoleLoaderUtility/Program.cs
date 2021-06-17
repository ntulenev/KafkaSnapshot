using System;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Serilog;

using KafkaSnapshot;
using System.Threading;
using System.Threading.Tasks;
using ConsoleLoaderUtility.Export;
using Confluent.Kafka;
using System.Collections.Generic;
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
                              services.AddSingleton<Func<string, (string, string)>>(x => (x, x));
                              services.AddSingleton<Func<IConsumer<Ignore, string>>>(() =>
                              {
                                  var conf = new ConsumerConfig
                                  {
                                      BootstrapServers = GetServers(),
                                      AutoOffsetReset = AutoOffsetReset.Earliest,
                                      GroupId = Guid.NewGuid().ToString()
                                  };

                                  return new ConsumerBuilder<Ignore, string>(conf).Build();
                              });
                              services.AddSingleton<ITopicWatermarkLoader>(sp =>
                              {
                                  var topicName = "test";
                                  var timeout = 1000;

                                  var adminConfig = new AdminClientConfig()
                                  {
                                      BootstrapServers = GetServers()
                                  };

                                  var adminClient = new AdminClientBuilder(adminConfig).Build();

                                  return new TopicWatermarkLoader(new TopicName(topicName), adminClient, timeout);
                              });

                              services.AddSingleton<IDataExporter<string>, DataExporter>();

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

                var loader = services.GetRequiredService<SnapshotLoader<string, string, string>>();
                var dump = await loader.LoadCompactSnapshotAsync(CancellationToken.None);
                var exporter = services.GetRequiredService<IDataExporter<string>>();
                await exporter.ExportAsync(dump, CancellationToken.None);
            }
        }

        private static string GetServers() => string.Join(",", _bootstrapServers);

        private static readonly List<string> _bootstrapServers = new List<string>()
        {
            //TODO Add test cluster servers 
        };
    }
}
