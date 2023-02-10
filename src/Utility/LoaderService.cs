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
        /// Creates application service.
        /// </summary>
        /// <param name="tool">Kafka loader tool.</param>
        /// <param name="hostApplicationLifetime">Application lifetime.</param>
        /// <exception cref="ArgumentNullException">Throws if aprams is null.</exception>
        public LoaderService(ILoaderTool tool, IHostApplicationLifetime hostApplicationLifetime)
        {
            _tool = tool ?? throw new ArgumentNullException(nameof(tool));
            _hostApplicationLifetime = hostApplicationLifetime ?? throw new ArgumentNullException(nameof(hostApplicationLifetime));
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
