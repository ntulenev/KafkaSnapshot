using System.Diagnostics;

using KafkaSnapshot.Export.Configuration;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Utility;

/// <summary>
/// Helpers for common config data.
/// </summary>
internal static class ConfigHelper
{
    /// <summary>
    /// Gets configuration for Kafka servers.
    /// </summary>
    public static BootstrapServersConfiguration GetBootstrapConfig(
        this IServiceProvider sp,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(sp);
        ArgumentNullException.ThrowIfNull(configuration);

        var section = configuration.GetSection(nameof(BootstrapServersConfiguration));
        var config = section.Get<BootstrapServersConfiguration>();

        Debug.Assert(config is not null);

        var validator = sp.GetRequiredService<IValidateOptions<BootstrapServersConfiguration>>();

        // Crutch to use IValidateOptions in manual generation logic.
        var validationResult = validator.Validate(string.Empty, config);
        return validationResult.Failed
            ? throw new OptionsValidationException(
                string.Empty,
                typeof(BootstrapServersConfiguration),
                [validationResult.FailureMessage])
            : config;
    }

    /// <summary>
    /// Gets configuration for Kafka topics.
    /// </summary>
    public static LoaderToolConfiguration GetLoaderConfig(
        this IServiceProvider sp,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(sp);
        ArgumentNullException.ThrowIfNull(configuration);

        var section = configuration.GetSection(nameof(LoaderToolConfiguration));
        var config = section.Get<LoaderToolConfiguration>();

        Debug.Assert(config is not null);

        var validator = sp.GetRequiredService<IValidateOptions<LoaderToolConfiguration>>();

        // Crutch to use IValidateOptions in manual generation logic.
        var validationResult = validator.Validate(string.Empty, config);
        return validationResult.Failed
            ? throw new OptionsValidationException(
                string.Empty,
                typeof(LoaderToolConfiguration),
                [validationResult.FailureMessage])
            : config;
    }

    /// <summary>
    /// Configures <see cref="LoaderToolConfiguration"/>
    /// </summary>
    public static IServiceCollection ConfigureLoaderTool(
        this IServiceCollection services,
        HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.Configure<LoaderToolConfiguration>(
            hostContext.Configuration.GetSection(nameof(LoaderToolConfiguration)));
        _ = services.AddSingleton<IValidateOptions<LoaderToolConfiguration>,
                                               LoaderToolConfigurationValidator>();
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

        _ = services.Configure<JsonFileDataExporterConfiguration>(
            hostContext.Configuration.GetSection(nameof(JsonFileDataExporterConfiguration)));
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

        _ = services.Configure<TopicWatermarkLoaderConfiguration>(
            hostContext.Configuration.GetSection(nameof(TopicWatermarkLoaderConfiguration)));
        _ = services.AddSingleton<IValidateOptions<BootstrapServersConfiguration>,
                                               BootstrapServersConfigurationValidator>();
        _ = services.AddSingleton<IValidateOptions<TopicWatermarkLoaderConfiguration>,
                                               TopicWatermarkLoaderConfigurationValidator>();
        _ = services.Configure<SnapshotLoaderConfiguration>(
            hostContext.Configuration.GetSection(nameof(SnapshotLoaderConfiguration)));
        return services;
    }

    /// <summary>
    /// Registers appsettings.
    /// </summary>
    public static void RegisterApplicationSettings(this IConfigurationBuilder builder) => _ = builder.AddJsonFile("appsettings.json", optional: true);
}
