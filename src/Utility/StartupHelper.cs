using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Confluent.Kafka;

using Serilog;

using KafkaSnapshot.Import;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Export.File.Json;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Filters;
using KafkaSnapshot.Models.Filters;

namespace KafkaSnapshot.Utility
{
    /// <summary>
    /// Helper utility class with services registration methods.
    /// </summary>
    public static class StartupHelper
    {
        /// <summary>
        /// Registers configuration and entry point of application.
        /// </summary>
        public static void AddTools(this IServiceCollection services, HostBuilderContext hostContext)
        {
            services.ConfigureLoaderTool(hostContext);
            services.AddScoped<LoaderTool>();
            services.AddScoped<LoaderConcurrentTool>();
            services.AddScoped<ILoaderTool>(sp =>
            {
                var config = sp.GetLoaderConfig(hostContext.Configuration);
                if (config.UseConcurrentLoad)
                {
                    return sp.GetRequiredService<LoaderConcurrentTool>();
                }
                else
                {
                    return sp.GetRequiredService<LoaderTool>();
                }
            });
        }

        /// <summary>
        /// Adds export providers.
        /// </summary>
        public static void AddExport(this IServiceCollection services)
        {
            services.AddSingleton<IDataExporter<long, OriginalKeyMarker, string, ExportedTopic>, OriginalKeyDataExporter<long>>();
            services.AddSingleton<IDataExporter<string, OriginalKeyMarker, string, ExportedTopic>, OriginalKeyDataExporter<string>>();
            services.AddSingleton<IDataExporter<string, JsonKeyMarker, string, ExportedTopic>, JsonKeyDataExporter>();
            services.AddSingleton<IFileSaver, FileSaver>();
        }

        /// <summary>
        /// Adds Serilog.
        /// </summary>
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

        /// <summary>
        /// Adds topic loaders.
        /// </summary>
        public static void AddTopicLoaders(this IServiceCollection services, HostBuilderContext hostContext)
        {
            services.AddSingleton<IKeyFiltersFactory<long>, NaiveKeyFiltersFactory<long>>();
            services.AddSingleton<IKeyFiltersFactory<string>, NaiveKeyFiltersFactory<string>>();
            services.AddSingleton(sp => CreateTopicLoaders(sp, hostContext.Configuration));
        }

        /// <summary>
        /// Add Kafka importers. 
        /// </summary>
        public static void AddImport(this IServiceCollection services, HostBuilderContext hostContext)
        {
            services.ConfigureImport(hostContext);

            services.AddSingleton(sp =>
            {
                var config = sp.GetBootstrapConfig(hostContext.Configuration);
                var servers = string.Join(",", config.BootstrapServers);
                var adminConfig = new AdminClientConfig()
                {
                    BootstrapServers = servers
                };
                return new AdminClientBuilder(adminConfig).Build();
            });

            services.AddSingleton<ITopicWatermarkLoader, TopicWatermarkLoader>();

            IConsumer<Key, string> createConsumer<Key>(IServiceProvider sp)
            {
                var config = sp.GetBootstrapConfig(hostContext.Configuration);
                var servers = string.Join(",", config.BootstrapServers);
                var conf = new ConsumerConfig
                {
                    BootstrapServers = servers,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    GroupId = Guid.NewGuid().ToString(),
                    EnableAutoCommit = false
                };

                return new ConsumerBuilder<Key, string>(conf).Build();
            }

            services.AddSingleton<Func<IConsumer<string, string>>>(sp => () => createConsumer<string>(sp));
            services.AddSingleton<Func<IConsumer<long, string>>>(sp => () => createConsumer<long>(sp));
            services.AddSingleton(typeof(ISnapshotLoader<,>), typeof(SnapshotLoader<,>));
        }

        private static ICollection<IProcessingUnit> CreateTopicLoaders(IServiceProvider sp, IConfiguration configuration)
        {
            var config = sp.GetLoaderConfig(configuration);

            return config.Topics.Select(topic => topic.KeyType switch
            {
                KeyType.Json => InitUnit<string, JsonKeyMarker>(topic, sp),
                KeyType.String => InitUnit<string, OriginalKeyMarker>(topic, sp),
                KeyType.Long => InitUnit<long, OriginalKeyMarker>(topic, sp),
                _ => throw new InvalidOperationException($"Invalid Key type {topic.KeyType} for processing.")
            }).ToList();
        }

        private static IProcessingUnit InitUnit<TKey, TMarker>(
                                TopicConfiguration topic,
                                IServiceProvider provider)
                                where TKey : notnull where TMarker : IKeyRepresentationMarker
           => ActivatorUtilities.CreateInstance<ProcessingUnit<TKey, TMarker, string>>(provider, topic.ConvertToProcess<TKey>());

    }
}
