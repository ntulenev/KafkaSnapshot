namespace KafkaSnapshot.Models.Configuration;

/// <summary>
/// Formats stable configuration validation errors.
/// </summary>
public static class ConfigurationValidationError
{
    /// <summary>
    /// Creates a validation error with a stable code prefix.
    /// </summary>
    public static string Create(string code, string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        ArgumentException.ThrowIfNullOrWhiteSpace(message);

        return $"{code}: {message}";
    }
}
