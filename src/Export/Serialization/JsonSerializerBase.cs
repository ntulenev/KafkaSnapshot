using System.Diagnostics;
using System.Text;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

namespace KafkaSnapshot.Export.Serialization;

/// <summary>
/// Base class for Json serialization.
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
        Logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Serializes data into JSON string.
    /// </summary>
    /// <param name="data">Data to serialize.</param>
    /// <returns>Serialized JSON.</returns>
    protected string SerializeData(object data)
    {
        Debug.Assert(data is not null);

        var sb = new StringBuilder();

        using var textWriter = new StringWriter(sb);

        using var jsonWriter = new JsonTextWriter(textWriter);

        Logger.LogTrace("Start serializing data.");

        _serializer.Serialize(jsonWriter, data);

        Logger.LogTrace("Finish serializing data.");

        return sb.ToString();
    }

    /// <summary>
    /// Serializes data into the provided stream.
    /// </summary>
    /// <param name="data">Data to serialize.</param>
    /// <param name="stream">Destination stream.</param>
    protected void SerializeDataToStream(object data, Stream stream)
    {
        Debug.Assert(data is not null);
        Debug.Assert(stream is not null);

        using var sw = new StreamWriter(stream);
        using var jsonWriter = new JsonTextWriter(sw);

        Logger.LogTrace("Start serializing data.");

        _serializer.Serialize(jsonWriter, data);

        Logger.LogTrace("Finish serializing data.");
    }

    private readonly JsonSerializer _serializer = new() { Formatting = Formatting.Indented };
    /// <summary>
    /// Logger instance.
    /// </summary>
    protected ILogger Logger { get; }
}
