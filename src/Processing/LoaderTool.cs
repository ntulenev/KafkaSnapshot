using Microsoft.Extensions.Logging;

using KafkaSnapshot.Abstractions.Processing;

namespace KafkaSnapshot.Processing;

/// <summary>
/// Tool that processes topics from Apache Kafka sequentially.
/// </summary>
public sealed class LoaderTool : ILoaderTool
{
    /// <summary>
    /// Creates <see cref="LoaderTool"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <param name="units">Processors for topics.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="logger"/> or
    /// <paramref name="units"/> is null.</exception>
    public LoaderTool(ILogger<LoaderTool> logger, IReadOnlyCollection<IProcessingUnit> units)
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
                "The utility starts loading {Count} Apache Kafka topics...",
                _units.Count);
        }

        var indexer = 0;

        foreach (var unit in _units)
        {
            var logScope = _logger.IsEnabled(LogLevel.Information)
                ? _logger.BeginScope("Topic {Topic}", unit.TopicName.Name)
                : null;

            using (logScope)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation(
                        "{Indexer}/{Count} Processing topic {TopicName}",
                        ++indexer,
                        _units.Count,
                        unit.TopicName.Name);
                }
                else
                {
                    indexer++;
                }

                ct.ThrowIfCancellationRequested();

                await unit.ProcessAsync(ct).ConfigureAwait(false);

                _logger.LogDebug("Finish processing topic.");
            }
        }

        _logger.LogInformation("Done.");
    }

    private readonly IReadOnlyCollection<IProcessingUnit> _units;
    private readonly ILogger _logger;

}
