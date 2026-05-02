using System.Text.Json;

using Microsoft.Extensions.Logging;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Base class for JSON serialization.
/// </summary>
public abstract class JsonSerializerBase
{
    /// <summary>
    /// Creates <see cref="JsonSerializerBase"/>.
    /// </summary>
    /// <param name="logger">Logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when logger is null.</exception>
    protected JsonSerializerBase(ILogger<JsonSerializerBase> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);

        Logger = logger;
    }

    /// <summary>
    /// Serializes data into JSON string.
    /// </summary>
    /// <param name="data">Data to serialize.</param>
    /// <returns>Serialized JSON.</returns>
    protected string SerializeData(object data)
    {
        ArgumentNullException.ThrowIfNull(data);

        Logger.LogTrace("Start serializing data.");

        var json = JsonSerializer.Serialize(data, _serializerOptions);

        Logger.LogTrace("Finish serializing data.");

        return json;
    }

    /// <summary>
    /// Serializes data into the provided stream.
    /// </summary>
    /// <param name="data">Data to serialize.</param>
    /// <param name="stream">Destination stream.</param>
    /// <param name="ct">The token for cancelling the operation.</param>
    protected async Task SerializeDataToStreamAsync(object data, Stream stream, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(stream);

        Logger.LogTrace("Start serializing data.");

        await JsonSerializer.SerializeAsync(stream, data, _serializerOptions, ct).ConfigureAwait(false);

        Logger.LogTrace("Finish serializing data.");
    }

    private static readonly JsonSerializerOptions _serializerOptions = new()
    {
        WriteIndented = true
    };
    /// <summary>
    /// Logger instance.
    /// </summary>
    protected ILogger Logger { get; }
}
