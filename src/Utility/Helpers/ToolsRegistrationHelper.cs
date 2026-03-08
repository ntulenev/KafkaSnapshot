using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers processing tool services.
/// </summary>
internal static class ToolsRegistrationHelper
{
    internal static void Register(IServiceCollection services, HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.ConfigureLoaderTool(hostContext);
        _ = services.AddScoped<LoaderTool>();
        _ = services.AddScoped<LoaderConcurrentTool>();
        _ = services.AddScoped(CreateLoaderTool);
    }

    private static ILoaderTool CreateLoaderTool(IServiceProvider serviceProvider)
    {
        var config = serviceProvider.GetRequiredService<IOptions<LoaderToolConfiguration>>().Value;
        return config.UseConcurrentLoad
            ? serviceProvider.GetRequiredService<LoaderConcurrentTool>()
            : serviceProvider.GetRequiredService<LoaderTool>();
    }
}
