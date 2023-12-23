namespace KafkaSnapshot.Export.Configuration;

/// <summary>
/// Configuration class for exporting data to a JSON file.
/// </summary>
public sealed class JsonFileDataExporterConfiguration
{
    /// <summary>
    /// Gets or sets a value indicating whether to use file streaming.
    /// </summary>
    public bool UseFileStreaming { get; set; }
}
