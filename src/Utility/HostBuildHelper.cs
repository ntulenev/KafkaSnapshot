using System.Diagnostics.CodeAnalysis;

using KafkaSnapshot.Utility.DependencyInjection;

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
        var builder = Host.CreateApplicationBuilder(args ?? []);
        _ = builder.Services.AddKafkaSnapshotUtility(builder.Configuration);

        return builder.Build();
    }
}
