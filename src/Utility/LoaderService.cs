using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.Hosting;

namespace KafkaSnapshot.Utility
{
    /// <summary>
    /// Service wrapper for application logic.
    /// </summary>
    public class LoaderService : BackgroundService
    {
        /// <summary>
        /// Initializes a new instance of the LoaderService.
        /// </summary>
        /// <param name="tool">A tool that implements the ILoaderTool interface 
        /// for processing data from Kafka.</param>
        /// <param name="hostApplicationLifetime">An object that provides a way 
        /// to interact with the lifetime of the host application.</param>
        /// <exception cref="System.ArgumentNullException">Thrown when the tool 
        /// or hostApplicationLifetime parameter is null.</exception>
        public LoaderService(
            ILoaderTool tool, 
            IHostApplicationLifetime hostApplicationLifetime)
        {
            _tool = tool ?? throw new ArgumentNullException(nameof(tool));
            _hostApplicationLifetime = hostApplicationLifetime ?? 
                throw new ArgumentNullException(nameof(hostApplicationLifetime));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await _tool.ProcessAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                _hostApplicationLifetime.StopApplication();
            }
        }

        private readonly ILoaderTool _tool;
        private readonly IHostApplicationLifetime _hostApplicationLifetime;
    }
}
