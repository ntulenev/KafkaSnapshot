using System;
using System.Collections.Generic;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Confluent.Kafka;

using Serilog;

using KafkaSnapshot.Export;
using KafkaSnapshot.Import;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Models.Processing;
using KafkaSnapshot.Export.File.Json;

namespace KafkaSnapshot.Utility
{
    public static class StartupHelper
    {
        public static void RegisterApplicationSettings(this IConfigurationBuilder builder)
        {
            builder.AddJsonFile("appsettings.json", optional: true);
        }

        public static void AddTools(this IServiceCollection services, HostBuilderContext hostContext)
        {
            services.AddScoped(typeof(LoaderTool));
            services.Configure<LoaderToolConfiguration>(hostContext.Configuration.GetSection(nameof(LoaderToolConfiguration)));
        }

        public static void AddExport(this IServiceCollection services)
        {
            services.AddSingleton(typeof(IDataExporter<,,>), typeof(JsonFileDataExporter<,,>));
        }

        public static void AddLogging(this IServiceCollection services, HostBuilderContext hostContext)
        {
            var logger = new LoggerConfiguration()
                             .ReadFrom.Configuration(hostContext.Configuration)
                             .CreateLogger();

            services.AddLogging(x =>
            {
                x.SetMinimumLevel(LogLevel.Information);
                x.AddSerilog(logger: logger, dispose: true);
            });
        }

        public static void AddTopicLoaders(this IServiceCollection services, HostBuilderContext hostContext)
        {
            services.AddSingleton(sp => CreateTopicLoaders(sp, hostContext.Configuration));
        }

        private static ICollection<IProcessingUnit> CreateTopicLoaders(IServiceProvider sp, IConfiguration configuration)
        {
            var list = new List<IProcessingUnit>();

            var section = configuration.GetSection(nameof(LoaderToolConfiguration));

            var config = section.Get<LoaderToolConfiguration>();

            // TODO Add config validation

            var servers = string.Join(",", config.BootstrapServers);

            IConsumer<Key, string> createConsumer<Key>()
            {
                var conf = new ConsumerConfig
                {
                    BootstrapServers = servers,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    GroupId = Guid.NewGuid().ToString(),
                };

                return new ConsumerBuilder<Key, string>(conf).Build();
            }

            void InitUnit<Key>(LoadedTopic topic) where Key : notnull
            {
                var adminConfig = new AdminClientConfig()
                {
                    BootstrapServers = servers
                };

                var adminClient = new AdminClientBuilder(adminConfig).Build();
                var wLoader = new TopicWatermarkLoader(new TopicName(topic.Name), adminClient, config.MetadataTimeout);

                list.Add(new ProcessingUnit<Key, string>(new ProcessingTopic(topic.Name, topic.ExportFileName),
                                            new SnapshotLoader<Key, string>(createConsumer<Key>, wLoader),
                                            sp.GetRequiredService<IDataExporter<Key, string, ExportedFileTopic>>()
                                            )
                        );
            }

            foreach (var topic in config.Topics)
            {
                switch (topic.KeyType)
                {
                    case KeyType.String: InitUnit<string>(topic); break;
                    case KeyType.Long: InitUnit<long>(topic); break;
                    default: throw new NotSupportedException($"Topic key type {topic.KeyType} not supported");
                }
            }

            return list;
        }
    }
}
