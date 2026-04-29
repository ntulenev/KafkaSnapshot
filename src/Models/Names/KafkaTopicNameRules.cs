namespace KafkaSnapshot.Models.Names;

/// <summary>
/// Shared Apache Kafka topic name validation rules.
/// </summary>
public static class KafkaTopicNameRules
{
    /// <summary>
    /// Maximum supported topic name length.
    /// </summary>
    public static int MaxLength { get; } = 249;

    /// <summary>
    /// Checks whether the topic name is valid.
    /// </summary>
    public static bool IsValid(string? name)
        => !string.IsNullOrWhiteSpace(name) &&
           name.Length <= MaxLength &&
           !name.Any(char.IsWhiteSpace) &&
           name.All(IsValidCharacter);

    private static bool IsValidCharacter(char value)
        => char.IsAsciiLetterOrDigit(value) ||
           value is '.' or '_' or '-';
}
