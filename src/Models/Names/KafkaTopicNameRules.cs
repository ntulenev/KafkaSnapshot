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
    public static bool IsValid(string? name) => Validate(name) == TopicNameValidationError.None;

    /// <summary>Returns the first failed topic name invariant.</summary>
    public static TopicNameValidationError Validate(string? name)
    {
        return name switch
        {
            null => TopicNameValidationError.Missing,
            _ when string.IsNullOrWhiteSpace(name) => TopicNameValidationError.Empty,
            _ when name.Any(char.IsWhiteSpace) => TopicNameValidationError.Whitespace,
            _ when name.Length > MaxLength => TopicNameValidationError.TooLong,
            _ when name.All(IsValidCharacter) => TopicNameValidationError.None,
            _ => TopicNameValidationError.InvalidCharacters
        };
    }

    private static bool IsValidCharacter(char value)
        => char.IsAsciiLetterOrDigit(value) ||
           value is '.' or '_' or '-';
}
