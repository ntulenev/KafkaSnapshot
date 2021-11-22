using Microsoft.Extensions.DependencyInjection;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Utility
{
    class Program
    {
        static async Task Main(string[] args)
        {
            using var serviceScope = HostBuildHelper.CreateHost().Services.CreateScope();
            var services = serviceScope.ServiceProvider;
            var tool = services.GetRequiredService<ILoaderTool>();
            await tool.ProcessAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
