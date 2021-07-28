using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Utility
{
    class Program
    {
        private static IHost CreateHost()
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

        static async Task Main(string[] args)
        {
            using var serviceScope = CreateHost().Services.CreateScope();
            var services = serviceScope.ServiceProvider;
            var tool = services.GetRequiredService<ILoaderTool>();
            await tool.ProcessAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
