using System.Diagnostics;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Hosting;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;
using KafkaSnapshot.Import.Configuration.Validation;
using KafkaSnapshot.Export.Configuration;

namespace KafkaSnapshot.Utility;

/// <summary>
/// Helpers for common config data.
/// </summary>
public static class ConfigHelper
{
    /// <summary>
    /// Gets configuration for Kafka servers.
    /// </summary>
    public static BootstrapServersConfiguration GetBootstrapConfig(
        this IServiceProvider sp, 
        IConfiguration configuration)
    {
        var section = configuration.GetSection(nameof(BootstrapServersConfiguration));
        var config = section.Get<BootstrapServersConfiguration>();

        Debug.Assert(config is not null);

        var validator = sp.GetRequiredService<IValidateOptions<BootstrapServersConfiguration>>();

        // Crutch to use IValidateOptions in manual generation logic.
        var validationResult = validator.Validate(string.Empty, config);
        if (validationResult.Failed)
        {
            throw new OptionsValidationException(
                string.Empty, 
                typeof(BootstrapServersConfiguration), 
                new[] { validationResult.FailureMessage });
        }

        return config;
    }

    /// <summary>
    /// Gets configuration for Kafka topics.
    /// </summary>
    public static LoaderToolConfiguration GetLoaderConfig(
        this IServiceProvider sp, 
        IConfiguration configuration)
    {
        var section = configuration.GetSection(nameof(LoaderToolConfiguration));
        var config = section.Get<LoaderToolConfiguration>();

        Debug.Assert(config is not null);

        var validator = sp.GetRequiredService<IValidateOptions<LoaderToolConfiguration>>();

        // Crutch to use IValidateOptions in manual generation logic.
        var validationResult = validator.Validate(string.Empty, config);
        if (validationResult.Failed)
        {
            throw new OptionsValidationException(
                string.Empty, 
                typeof(LoaderToolConfiguration),
                new[] { validationResult.FailureMessage });
        }

        return config;
    }

    /// <summary>
    /// Confugures <see cref="LoaderToolConfiguration"/>
    /// </summary>
    public static IServiceCollection ConfigureLoaderTool(
        this IServiceCollection services, 
        HostBuilderContext hostContext)
    {
        services.Configure<LoaderToolConfiguration>(
            hostContext.Configuration.GetSection(nameof(LoaderToolConfiguration)));
        services.AddSingleton<IValidateOptions<LoaderToolConfiguration>, 
                                               LoaderToolConfigurationValidator>();
        return services;
    }

    /// <summary>
    /// Confugures <see cref="JsonFileDataExporterConfiguration"/>
    /// </summary>
    public static IServiceCollection ConfigureExport(
        this IServiceCollection services, 
        HostBuilderContext hostContext)
    {
        services.Configure<JsonFileDataExporterConfiguration>(
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
        services.Configure<TopicWatermarkLoaderConfiguration>(
            hostContext.Configuration.GetSection(nameof(TopicWatermarkLoaderConfiguration)));
        services.AddSingleton<IValidateOptions<BootstrapServersConfiguration>, 
                                               BootstrapServersConfigurationValidator>();
        services.AddSingleton<IValidateOptions<TopicWatermarkLoaderConfiguration>, 
                                               TopicWatermarkLoaderConfigurationValidator>();
        services.Configure<SnapshotLoaderConfiguration>(
            hostContext.Configuration.GetSection(nameof(SnapshotLoaderConfiguration)));
        return services;
    }

    /// <summary>
    /// Registers appsettings.
    /// </summary>
    public static void RegisterApplicationSettings(this IConfigurationBuilder builder)
    {
        builder.AddJsonFile("appsettings.json", optional: true);
    }
}
