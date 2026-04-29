using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Serilog;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers logging services.
/// </summary>
internal static class LoggingRegistrationHelper
{
    internal static IServiceCollection Register(IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        var logger = new LoggerConfiguration()
                         .ReadFrom.Configuration(configuration)
                         .CreateLogger();

        _ = services.AddLogging(builder =>
        {
            _ = builder.SetMinimumLevel(LogLevel.Information);
            _ = builder.AddSerilog(logger: logger, dispose: true);
        });

        return services;
    }
}
