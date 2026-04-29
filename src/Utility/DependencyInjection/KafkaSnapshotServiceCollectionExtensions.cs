using KafkaSnapshot.Utility.Helpers;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaSnapshot.Utility.DependencyInjection;

/// <summary>
/// Registers KafkaSnapshot application services.
/// </summary>
public static class KafkaSnapshotServiceCollectionExtensions
{
    /// <summary>
    /// Adds import services.
    /// </summary>
    public static IServiceCollection AddKafkaSnapshotImport(
        this IServiceCollection services,
        IConfiguration configuration)
        => ImportRegistrationHelper.Register(services, configuration);

    /// <summary>
    /// Adds processing services.
    /// </summary>
    public static IServiceCollection AddKafkaSnapshotProcessing(
        this IServiceCollection services,
        IConfiguration configuration)
        => ToolsRegistrationHelper.Register(services, configuration);

    /// <summary>
    /// Adds export services.
    /// </summary>
    public static IServiceCollection AddKafkaSnapshotExport(
        this IServiceCollection services,
        IConfiguration configuration)
        => ExportRegistrationHelper.Register(services, configuration);

    /// <summary>
    /// Adds utility host services.
    /// </summary>
    public static IServiceCollection AddKafkaSnapshotUtility(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        _ = services.AddKafkaSnapshotProcessing(configuration);
        _ = services.AddKafkaSnapshotImport(configuration);
        _ = services.AddKafkaSnapshotExport(configuration);
        _ = TopicLoadersRegistrationHelper.Register(services);
        _ = LoggingRegistrationHelper.Register(services, configuration);
        _ = services.AddHostedService<LoaderService>();

        return services;
    }
}
