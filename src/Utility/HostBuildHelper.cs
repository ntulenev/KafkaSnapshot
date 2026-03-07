using System.Diagnostics.CodeAnalysis;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KafkaSnapshot.Utility;

/// <summary>
/// Helper class for IHost creation.
/// </summary>
[SuppressMessage("Performance", "CA1515")]
public static class HostBuildHelper
{
    /// <summary>
    /// Creates default host for app.
    /// </summary>
    /// <param name="args">Command-line arguments.</param>
    public static IHost CreateHost(string[]? args = null)
    {
        var builder = Host.CreateDefaultBuilder(args ?? [])
               .ConfigureServices((hostContext, services) =>
               {
                   services.AddTools(hostContext);
                   services.AddImport(hostContext);
                   services.AddExport(hostContext);
                   services.AddTopicLoaders();
                   services.AddLogging(hostContext);
                   _ = services.AddHostedService<LoaderService>();
               });

        return builder.Build();
    }
}
