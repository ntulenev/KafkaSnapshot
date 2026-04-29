using System.Diagnostics.CodeAnalysis;

using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaSnapshot.Utility;

/// <summary>
/// Service wrapper for application logic.
/// </summary>
[SuppressMessage("Performance", "CA1515")]
public sealed class LoaderService : BackgroundService
{
    /// <summary>
    /// Initializes a new instance of the LoaderService.
    /// </summary>
    /// <param name="tool">A tool that implements the ILoaderTool interface 
    /// for processing data from Kafka.</param>
    /// <param name="hostApplicationLifetime">An object that provides a way 
    /// to interact with the lifetime of the host application.</param>
    /// <param name="logger">Logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when the tool 
    /// or hostApplicationLifetime parameter is null.</exception>
    public LoaderService(
        ILoaderTool tool,
        IHostApplicationLifetime hostApplicationLifetime,
        ILogger<LoaderService> logger)
    {
        _tool = tool ?? throw new ArgumentNullException(nameof(tool));
        _hostApplicationLifetime = hostApplicationLifetime ??
            throw new ArgumentNullException(nameof(hostApplicationLifetime));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    protected async override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("KafkaSnapshot loader service is starting.");

        try
        {
            await _tool.ProcessAsync(stoppingToken).ConfigureAwait(false);
            _logger.LogInformation("KafkaSnapshot loader service completed.");
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("KafkaSnapshot loader service was canceled.");
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "KafkaSnapshot loader service failed.");
            throw;
        }
        finally
        {
            _hostApplicationLifetime.StopApplication();
        }
    }

    private readonly ILoaderTool _tool;
    private readonly IHostApplicationLifetime _hostApplicationLifetime;
    private readonly ILogger<LoaderService> _logger;
}
