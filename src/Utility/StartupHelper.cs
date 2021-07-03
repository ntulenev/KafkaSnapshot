﻿using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Confluent.Kafka;

using Serilog;

using KafkaSnapshot.Import;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Export.File.Json;
using KafkaSnapshot.Processing.Configuration.Validation;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Import.Filters;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Import.Configuration.Validation;

namespace KafkaSnapshot.Utility
{
    /// <summary>
    /// Helper utility class with services registration methods.
    /// </summary>
    public static class StartupHelper
    {
        /// <summary>
        /// Registers appsettings.
        /// </summary>
        public static void RegisterApplicationSettings(this IConfigurationBuilder builder)
        {
            builder.AddJsonFile("appsettings.json", optional: true);
        }

        /// <summary>
        /// Registers configuration and entry point of application.
        /// </summary>
        public static void AddTools(this IServiceCollection services, HostBuilderContext hostContext)
        {
            services.AddScoped(typeof(LoaderTool));
            services.Configure<LoaderToolConfiguration>(hostContext.Configuration.GetSection(nameof(LoaderToolConfiguration)));
            services.Configure<TopicWatermarkLoaderConfiguration>(hostContext.Configuration.GetSection(nameof(TopicWatermarkLoaderConfiguration)));
            services.AddSingleton<IValidateOptions<LoaderToolConfiguration>, LoaderToolConfigurationValidator>();

        }

        /// <summary>
        /// Adds export providers.
        /// </summary>
        public static void AddExport(this IServiceCollection services)
        {
            services.AddSingleton<IDataExporter<long, OriginalKeyMarker, string, ExportedTopic>, OriginalKeyJsonValueDataExporter<long>>();
            services.AddSingleton<IDataExporter<string, OriginalKeyMarker, string, ExportedTopic>, OriginalKeyJsonValueDataExporter<string>>();
            services.AddSingleton<IDataExporter<string, JsonKeyMarker, string, ExportedTopic>, JsonKeyJsonValueDataExporter>();
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
            services.AddSingleton<IValidateOptions<BootstrapServersConfiguration>, BootstrapServersConfigurationValidator>();
            services.AddSingleton<IValidateOptions<TopicWatermarkLoaderConfiguration>, TopicWatermarkLoaderConfigurationValidator>();

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
                                LoadedTopic topic,
                                IServiceProvider provider)
                                where TKey : notnull where TMarker : IKeyRepresentationMarker
        {
            // TODO: Recator method for nice DI.
            return new ProcessingUnit<TKey, TMarker, string>(
                                        provider.GetRequiredService<ILogger<ProcessingUnit<TKey, TMarker, string>>>(),
                                        topic.ConvertToProcess<TKey>(),
                                        provider.GetRequiredService<ISnapshotLoader<TKey, string>>(),
                                        provider.GetRequiredService<IDataExporter<TKey, TMarker, string, ExportedTopic>>(),
                                        provider.GetRequiredService<IKeyFiltersFactory<TKey>>()
                                        );
        }
    }
}
