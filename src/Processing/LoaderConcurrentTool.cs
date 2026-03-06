using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Processing;

/// <summary>
/// Tool that process topics from Apache Kafka (in concurrent mode).
/// </summary>
public sealed class LoaderConcurrentTool : ILoaderTool
{

    /// <summary>
    /// Creates <see cref="LoaderConcurrentTool"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <param name="units">Processors for topics.</param>
    /// <exception cref="ArgumentNullException"></exception>
    public LoaderConcurrentTool(
            ILogger<LoaderConcurrentTool> logger,
            IReadOnlyCollection<IProcessingUnit> units)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _units = units ?? throw new ArgumentNullException(nameof(units));

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

        await Task.WhenAll(_units.Select(async unit =>
        {
            using var _ = _logger.BeginScope("Topic {Topic}", unit.TopicName.Name);

            _logger.LogInformation("Start processing topic.");

            ct.ThrowIfCancellationRequested();

            await unit.ProcessAsync(ct).ConfigureAwait(false);

            _logger.LogInformation("Finish processing topic.");

        }));

        _logger.LogInformation("Done.");
    }

    private readonly IReadOnlyCollection<IProcessingUnit> _units;
    private readonly ILogger _logger;

}
