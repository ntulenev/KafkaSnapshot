using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using KafkaSnapshot.Models.Processing;
using KafkaSnapshot.Export.File.Json;
using KafkaSnapshot.Processing.Configuration.Validation;
using KafkaSnapshot.Models.Export;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Import.Filters;
using KafkaSnapshot.Abstractions.Filters;

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

        private static LoaderToolConfiguration GetConfig(IServiceProvider sp, IConfiguration configuration)
        {
            var section = configuration.GetSection(nameof(LoaderToolConfiguration));
            var config = section.Get<LoaderToolConfiguration>();

            Debug.Assert(config is not null);

            var validator = sp.GetRequiredService<IValidateOptions<LoaderToolConfiguration>>();

            // Crutch to use IValidateOptions in manual generation logic.
            var validationResult = validator.Validate(string.Empty, config);
            if (validationResult.Failed)
            {
                throw new OptionsValidationException
                    (string.Empty, config.GetType(), new[] { validationResult.FailureMessage });
            }

            return config;
        }

        /// <summary>
        /// Creates topic loaders from config
        /// </summary>
        private static ICollection<IProcessingUnit> CreateTopicLoaders(IServiceProvider sp, IConfiguration configuration)
        {
            var config = GetConfig(sp, configuration);
            return config.Topics.Select(topic => topic.KeyType switch
            {
                KeyType.Json => InitUnit<string, JsonKeyMarker>(topic, sp, config),
                KeyType.String => InitUnit<string, OriginalKeyMarker>(topic, sp, config),
                KeyType.Long => InitUnit<long, OriginalKeyMarker>(topic, sp, config),
                _ => throw new InvalidOperationException($"Invalid Key type {topic.KeyType} for processing.")
            }).ToList();
        }

        /// <summary>
        /// Create single loader
        /// </summary>
        private static IProcessingUnit InitUnit<TKey, TMarker>
                                (LoadedTopic topic, IServiceProvider provider, LoaderToolConfiguration config)
                                where TKey : notnull where TMarker : IKeyRepresentationMarker
        {
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

            var list = new List<ProcessingUnit<TKey, TMarker, string>>();

            var adminConfig = new AdminClientConfig()
            {
                BootstrapServers = servers
            };

            var adminClient = new AdminClientBuilder(adminConfig).Build();

            var wLoader = new TopicWatermarkLoader(new TopicName(topic.Name), adminClient, config.MetadataTimeout);

            var pTopic = new ProcessingTopic(topic.Name, topic.ExportFileName, topic.Compacting == CompactingMode.On);

            var filterFactory = provider.GetRequiredService<IKeyFiltersFactory<TKey>>();

            var typedFilterValue = topic.FilterValue is not null ?
                                    (TKey)Convert.ChangeType(topic.FilterValue, typeof(TKey))
                                    :
                                    default;

            var loader = new SnapshotLoader<TKey, string>(
                    provider.GetRequiredService<ILogger<SnapshotLoader<TKey, string>>>(),
                    createConsumer<TKey>,
                    wLoader,
                    filterFactory.Create(topic.FilterType, typedFilterValue!));

            return new ProcessingUnit<TKey, TMarker, string>(
                                        provider.GetRequiredService<ILogger<ProcessingUnit<TKey, TMarker, string>>>(),
                                        pTopic,
                                        loader,
                                        provider.GetRequiredService<IDataExporter<TKey, TMarker, string, ExportedTopic>>()
                                        );
        }
    }
}
