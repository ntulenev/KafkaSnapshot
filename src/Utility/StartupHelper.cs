using Confluent.Kafka;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Abstractions.Filters;
using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Abstractions.Sorting;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.File.Output;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Filters;
using KafkaSnapshot.Import;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Encoders;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Sorting;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Serilog;

namespace KafkaSnapshot.Utility;

/// <summary>
/// Helper utility class with services registration methods.
/// </summary>
internal static class StartupHelper
{
    /// <summary>
    /// Register configuration and entry point of application.
    /// </summary>
    public static void AddTools(this IServiceCollection services, HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.ConfigureLoaderTool(hostContext);
        _ = services.AddScoped<LoaderTool>();
        _ = services.AddScoped<LoaderConcurrentTool>();
        _ = services.AddScoped<ILoaderTool>(sp =>
        {
            var config = sp.GetRequiredService<IOptions<LoaderToolConfiguration>>().Value;
            return config.UseConcurrentLoad
                ? sp.GetRequiredService<LoaderConcurrentTool>()
                : sp.GetRequiredService<LoaderTool>();
        });
    }

    /// <summary>
    /// Add export providers.
    /// </summary>
    public static void AddExport(this IServiceCollection services, HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.ConfigureExport(hostContext);

        _ = services.AddSingleton<ISerializer<string, string, IgnoreKeyMarker>, IgnoreKeySerializer>();
        _ = services.AddSingleton<ISerializer<string, string, JsonKeyMarker>, JsonKeySerializer>();
        _ = services.AddSingleton<ISerializer<string, string, OriginalKeyMarker>, OriginalKeySerializer<string>>();
        _ = services.AddSingleton<ISerializer<long, string, OriginalKeyMarker>, OriginalKeySerializer<long>>();
        _ = services.AddSingleton(typeof(IDataExporter<,,,>), typeof(JsonFileDataExporter<,,,>));
        _ = services.AddSingleton<IFileSaver, FileSaver>();
        _ = services.AddSingleton<IFileStreamProvider, FileStreamProvider>();
    }

    /// <summary>
    /// Add Serilog.
    /// </summary>
    public static void AddLogging(this IServiceCollection services, HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        var logger = new LoggerConfiguration()
                         .ReadFrom.Configuration(hostContext.Configuration)
                         .CreateLogger();

        _ = services.AddLogging(x =>
        {
            _ = x.SetMinimumLevel(LogLevel.Information);
            _ = x.AddSerilog(logger: logger, dispose: true);
        });
    }

    /// <summary>
    /// Add topic loaders.
    /// </summary>
    public static void AddTopicLoaders(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        _ = services.AddSingleton<IKeyFiltersFactory<long>, NaiveKeyFiltersFactory<long>>();
        _ = services.AddSingleton<IKeyFiltersFactory<string>, NaiveKeyFiltersFactory<string>>();
        _ = services.AddSingleton<IValueFilterFactory<string>, NaiveValueFiltersFactory<string>>();
        _ = services.AddSingleton(CreateTopicLoaders);
    }

    /// <summary>
    /// Add Kafka importers. 
    /// </summary>
    public static void AddImport(this IServiceCollection services, HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.ConfigureImport(hostContext);

        _ = services.AddSingleton<IMessageEncoder<byte[], string>, ByteMessageEncoder>();

        _ = services.AddSingleton(sp =>
        {
            var config = sp.GetRequiredService<IOptions<BootstrapServersConfiguration>>().Value;
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

        _ = services.AddSingleton<ITopicWatermarkLoader, TopicWatermarkLoader>();

        IConsumer<Key, byte[]> createConsumer<Key>(IServiceProvider sp)
        {
            var config = sp.GetRequiredService<IOptions<BootstrapServersConfiguration>>().Value;
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

            return new ConsumerBuilder<Key, byte[]>(conf).Build();
        }

        _ = services.AddSingleton<Func<IConsumer<string, byte[]>>>(
            sp => () => createConsumer<string>(sp));
        _ = services.AddSingleton<Func<IConsumer<long, byte[]>>>(
            sp => () => createConsumer<long>(sp));
        _ = services.AddSingleton(typeof(ISnapshotLoader<,>), typeof(SnapshotLoader<,>));

        IMessageSorter<TKey, TValue> createSorter<TKey, TValue>(IServiceProvider sp)
            where TKey : notnull where TValue : notnull
        {
            var config = sp.GetRequiredService<IOptions<LoaderToolConfiguration>>().Value;
            return new MessageSorter<TKey, TValue>(
                new Models.Sorting.SortingParams(config.GlobalMessageSort, config.GlobalSortOrder));
        }
        _ = services.AddSingleton(createSorter<string, string>);
        _ = services.AddSingleton(createSorter<long, string>);
    }

    private static IReadOnlyCollection<IProcessingUnit> CreateTopicLoaders(IServiceProvider sp)
    {
        var config = sp.GetRequiredService<IOptions<LoaderToolConfiguration>>().Value;

        return [.. config.Topics.Select(topic => topic.KeyType switch
        {
            KeyType.Ignored => (IProcessingUnit)InitUnit<string, IgnoreKeyMarker>(topic, sp),
            KeyType.Json => InitUnit<string, JsonKeyMarker>(topic, sp),
            KeyType.String => InitUnit<string, OriginalKeyMarker>(topic, sp),
            KeyType.Long => InitUnit<long, OriginalKeyMarker>(topic, sp),
            _ => throw new InvalidOperationException($"Invalid Key type " +
            $"{topic.KeyType} for processing.")
        })];
    }

    private static ProcessingUnit<TKey, TMarker, string> InitUnit<TKey, TMarker>(
                            TopicConfiguration topic,
                            IServiceProvider provider)
                            where TKey : notnull where TMarker : IKeyRepresentationMarker
       => ActivatorUtilities.CreateInstance<ProcessingUnit<TKey, TMarker, string>>(
           provider,
           topic.ConvertToProcess<TKey>());

}
