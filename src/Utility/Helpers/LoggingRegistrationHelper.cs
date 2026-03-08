using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Serilog;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers logging services.
/// </summary>
internal static class LoggingRegistrationHelper
{
    internal static void Register(IServiceCollection services, HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        var logger = new LoggerConfiguration()
                         .ReadFrom.Configuration(hostContext.Configuration)
                         .CreateLogger();

        _ = services.AddLogging(builder =>
        {
            _ = builder.SetMinimumLevel(LogLevel.Information);
            _ = builder.AddSerilog(logger: logger, dispose: true);
        });
    }
}
