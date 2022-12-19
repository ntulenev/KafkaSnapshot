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
using KafkaSnapshot.Export.File.Output;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Filters;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Sorting;

namespace KafkaSnapshot.Utility;

/// <summary>
/// Helper utility class with services registration methods.
/// </summary>
public static class StartupHelper
{
    /// <summary>
    /// Register configuration and entry point of application.
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
    /// Add export providers.
    /// </summary>
    public static void AddExport(this IServiceCollection services)
    {
        services.AddSingleton<ISerializer<string, string, IgnoreKeyMarker>, IgnoreKeySerializer>();
        services.AddSingleton<ISerializer<string, string, JsonKeyMarker>, JsonKeySerializer>();
        services.AddSingleton<ISerializer<string, string, OriginalKeyMarker>, OriginalKeySerializer<string>>();
        services.AddSingleton<ISerializer<long, string, OriginalKeyMarker>, OriginalKeySerializer<long>>();
        services.AddSingleton(typeof(IDataExporter<,,,>), typeof(JsonFileDataExporter<,,,>));
        services.AddSingleton<IFileSaver, FileSaver>();
    }

    /// <summary>
    /// Add Serilog.
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
    /// Add topic loaders.
    /// </summary>
    public static void AddTopicLoaders(this IServiceCollection services, HostBuilderContext hostContext)
    {
        services.AddSingleton<IKeyFiltersFactory<long>, NaiveKeyFiltersFactory<long>>();
        services.AddSingleton<IKeyFiltersFactory<string>, NaiveKeyFiltersFactory<string>>();
        services.AddSingleton<IValueFilterFactory<string>, NaiveValueFiltersFactory<string>>();
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
                BootstrapServers = servers,
                SecurityProtocol = config.SecurityProtocol,
                SaslMechanism = config.SASLMechanism,
                SaslUsername = config.Username,
                SaslPassword = config.Password
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
                EnableAutoCommit = false,
                SecurityProtocol = config.SecurityProtocol,
                SaslMechanism = config.SASLMechanism,
                SaslUsername = config.Username,
                SaslPassword = config.Password
            };

            return new ConsumerBuilder<Key, string>(conf).Build();
        }

        services.AddSingleton<Func<IConsumer<string, string>>>(sp => () => createConsumer<string>(sp));
        services.AddSingleton<Func<IConsumer<long, string>>>(sp => () => createConsumer<long>(sp));
        services.AddSingleton(typeof(ISnapshotLoader<,>), typeof(SnapshotLoader<,>));

        IMessageSorter<TKey, TValue> createSorter<TKey, TValue>(IServiceProvider sp)
            where TKey : notnull where TValue : notnull
        {
            var config = sp.GetLoaderConfig(hostContext.Configuration);
            return new MessageSorter<TKey, TValue>
                (new Models.Sorting.SortingParams(config.GlobalMessageSort, config.GlobalSortOrder));
        }
        services.AddSingleton(sp => createSorter<string, string>(sp));
        services.AddSingleton(sp => createSorter<long, string>(sp));
    }

    private static IReadOnlyCollection<IProcessingUnit> CreateTopicLoaders(IServiceProvider sp, IConfiguration configuration)
    {
        var config = sp.GetLoaderConfig(configuration);

        return config.Topics.Select(topic => topic.KeyType switch
        {
            KeyType.Ignored => InitUnit<string, IgnoreKeyMarker>(topic, sp),
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
