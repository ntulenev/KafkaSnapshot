using System;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

using KafkaSnapshot.Processing;

namespace KafkaSnapshot.ConsoleLoaderUtility
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
                       services.AddExport();
                       services.AddTopicLoaders(hostContext);
                       services.AddLogging(hostContext);
                   });

            return builder.Build();
        }

        static async Task Main(string[] args)
        {
            try
            {
                using var serviceScope = CreateHost().Services.CreateScope();
                var services = serviceScope.ServiceProvider;
                var tool = services.GetRequiredService<LoaderTool>();

                await tool.ProcessAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error {ex}");
            }
        }
    }
}
