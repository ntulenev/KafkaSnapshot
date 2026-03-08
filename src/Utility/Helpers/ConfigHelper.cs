using KafkaSnapshot.Export.Configuration;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Helpers for common config data.
/// </summary>
internal static class ConfigHelper
{
    /// <summary>
    /// Configures <see cref="LoaderToolConfiguration"/>
    /// </summary>
    public static IServiceCollection ConfigureLoaderTool(
        this IServiceCollection services,
        HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.AddSingleton<IValidateOptions<LoaderToolConfiguration>,
                                               LoaderToolConfigurationValidator>();
        _ = services.AddOptions<LoaderToolConfiguration>()
            .Bind(hostContext.Configuration.GetSection(nameof(LoaderToolConfiguration)))
            .ValidateOnStart();

        return services;
    }

    /// <summary>
    /// Configures <see cref="JsonFileDataExporterConfiguration"/>
    /// </summary>
    public static IServiceCollection ConfigureExport(
        this IServiceCollection services,
        HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.AddOptions<JsonFileDataExporterConfiguration>()
            .Bind(hostContext.Configuration.GetSection(nameof(JsonFileDataExporterConfiguration)))
            .ValidateOnStart();

        return services;
    }

    /// <summary>
    /// Configures Kafka import settings.
    /// </summary>
    public static IServiceCollection ConfigureImport(
        this IServiceCollection services,
        HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.AddSingleton<IValidateOptions<BootstrapServersConfiguration>,
                                               BootstrapServersConfigurationValidator>();
        _ = services.AddSingleton<IValidateOptions<TopicWatermarkLoaderConfiguration>,
                                               TopicWatermarkLoaderConfigurationValidator>();
        _ = services.AddSingleton<IValidateOptions<SnapshotLoaderConfiguration>,
                                               SnapshotLoaderConfigurationValidator>();

        _ = services.AddOptions<BootstrapServersConfiguration>()
            .Bind(hostContext.Configuration.GetSection(nameof(BootstrapServersConfiguration)))
            .ValidateOnStart();
        _ = services.AddOptions<TopicWatermarkLoaderConfiguration>()
            .Bind(hostContext.Configuration.GetSection(nameof(TopicWatermarkLoaderConfiguration)))
            .ValidateOnStart();
        _ = services.AddOptions<SnapshotLoaderConfiguration>()
            .Bind(hostContext.Configuration.GetSection(nameof(SnapshotLoaderConfiguration)))
            .ValidateOnStart();

        return services;
    }

    /// <summary>
    /// Registers appsettings.
    /// </summary>
    public static void RegisterApplicationSettings(this IConfigurationBuilder builder) => _ = builder.AddJsonFile("appsettings.json", optional: true);
}
