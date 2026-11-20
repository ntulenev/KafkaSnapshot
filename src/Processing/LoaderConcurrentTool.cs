using KafkaSnapshot.Abstractions.Processing;

using Microsoft.Extensions.Logging;

namespace KafkaSnapshot.Processing;

/// <summary>
/// Tool that processes topics from Apache Kafka concurrently.
/// </summary>
public sealed class LoaderConcurrentTool : ILoaderTool
{

    /// <summary>
    /// Creates <see cref="LoaderConcurrentTool"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <param name="units">Processors for topics.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="logger"/> or
    /// <paramref name="units"/> is null.</exception>
    public LoaderConcurrentTool(
            ILogger<LoaderConcurrentTool> logger,
            IReadOnlyCollection<IProcessingUnit> units)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(units);

        _logger = logger;
        _units = units;

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Instance created for {Count} unit(s).", _units.Count);
        }
    }

    /// <inheritdoc/>
    public async Task ProcessAsync(CancellationToken ct)
    {
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation(
                "The utility starts loading {Count} Apache Kafka topics concurrently...",
                _units.Count);
        }

        ct.ThrowIfCancellationRequested();
        using var stopped = CancellationTokenSource.CreateLinkedTokenSource(ct);
#pragma warning disable CA2025 // WhenAll joins every unit before disposing the shared cancellation source.
        await Task.WhenAll(_units.Select(unit => Task.Run(async () =>
        {
            try
            {
                stopped.Token.ThrowIfCancellationRequested();
                using var _ = _logger.BeginScope("Topic {Topic}", unit.TopicName.Name);

                _logger.LogInformation("Start processing topic.");

                await unit.ProcessAsync(stopped.Token).ConfigureAwait(false);

                _logger.LogInformation("Finish processing topic.");
            }
            catch
            {
                await stopped.CancelAsync().ConfigureAwait(false);
                throw;
            }
        }, stopped.Token))).ConfigureAwait(false);
#pragma warning restore CA2025

        _logger.LogInformation("Done.");
    }

    private readonly IReadOnlyCollection<IProcessingUnit> _units;
    private readonly ILogger _logger;

}
