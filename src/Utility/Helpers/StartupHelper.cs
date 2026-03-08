using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Helper utility class with services registration methods.
/// </summary>
internal static class StartupHelper
{
    /// <summary>
    /// Register configuration and entry point of application.
    /// </summary>
    public static void AddTools(this IServiceCollection services, HostBuilderContext hostContext)
        => ToolsRegistrationHelper.Register(services, hostContext);

    /// <summary>
    /// Add export providers.
    /// </summary>
    public static void AddExport(this IServiceCollection services, HostBuilderContext hostContext)
        => ExportRegistrationHelper.Register(services, hostContext);

    /// <summary>
    /// Add Serilog.
    /// </summary>
    public static void AddLogging(this IServiceCollection services, HostBuilderContext hostContext)
        => LoggingRegistrationHelper.Register(services, hostContext);

    /// <summary>
    /// Add topic loaders.
    /// </summary>
    public static void AddTopicLoaders(this IServiceCollection services)
        => TopicLoadersRegistrationHelper.Register(services, CreateTopicLoaders);

    /// <summary>
    /// Add Kafka importers. 
    /// </summary>
    public static void AddImport(this IServiceCollection services, HostBuilderContext hostContext)
        => ImportRegistrationHelper.Register(services, hostContext);

    private static IReadOnlyCollection<IProcessingUnit> CreateTopicLoaders(IServiceProvider sp)
        => TopicLoadersRegistrationHelper.CreateTopicLoaders(sp);
}
