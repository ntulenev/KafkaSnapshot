using Newtonsoft.Json.Linq;

using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters;

/// <summary>
/// Json equals filter.
/// </summary>
public class JsonEqualsFilter : IDataFilter<string>
{
    /// <summary>
    /// Creates <see cref="JsonEqualsFilter"/>.
    /// </summary>
    /// <param name="sample">Data sample.</param>
    public JsonEqualsFilter(string sample)
    {
        ArgumentNullException.ThrowIfNull(sample);

        _sample = JObject.Parse(sample);
    }

    /// <inheritdoc/>
    public bool IsMatch(string data)
    {
        ArgumentNullException.ThrowIfNull(data);

        var jsonKey = JObject.Parse(data);


        return JToken.DeepEquals(jsonKey, _sample);
    }

    private readonly JObject _sample;
}
