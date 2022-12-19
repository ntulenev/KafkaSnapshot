using Microsoft.Extensions.Hosting;

namespace KafkaSnapshot.Utility;

/// <summary>
/// Helper class for IHost creation.
/// </summary>
public static class HostBuildHelper
{
    /// <summary>
    /// Creates default host for app.
    /// </summary>
    public static IHost CreateHost()
    {
        var builder = new HostBuilder()
               .ConfigureAppConfiguration((hostingContext, config) =>
               {
                   config.RegisterApplicationSettings();
               })
               .ConfigureServices((hostContext, services) =>
               {
                   services.AddTools(hostContext);
                   services.AddImport(hostContext);
                   services.AddExport();
                   services.AddTopicLoaders(hostContext);
                   services.AddLogging(hostContext);
               });

        return builder.Build();
    }
}
