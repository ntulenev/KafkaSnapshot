using KafkaSnapshot.Abstractions.Processing;
using KafkaSnapshot.Processing;
using KafkaSnapshot.Processing.Configuration;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers processing tool services.
/// </summary>
internal static class ToolsRegistrationHelper
{
    internal static IServiceCollection Register(IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        _ = services.ConfigureLoaderTool(configuration);
        _ = services.AddScoped<LoaderTool>();
        _ = services.AddScoped<LoaderConcurrentTool>();
        _ = services.AddScoped(CreateLoaderTool);

        return services;
    }

    private static ILoaderTool CreateLoaderTool(IServiceProvider serviceProvider)
    {
        var config = serviceProvider.GetRequiredService<IOptions<LoaderToolConfiguration>>().Value;
        return config.UseConcurrentLoad
            ? serviceProvider.GetRequiredService<LoaderConcurrentTool>()
            : serviceProvider.GetRequiredService<LoaderTool>();
    }
}
