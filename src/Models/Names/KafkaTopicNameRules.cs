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
        if (name is null)
        {
            return TopicNameValidationError.Missing;
        }

        if (string.IsNullOrWhiteSpace(name))
        {
            return TopicNameValidationError.Empty;
        }

        if (name.Any(char.IsWhiteSpace))
        {
            return TopicNameValidationError.Whitespace;
        }

        if (name.Length > MaxLength)
        {
            return TopicNameValidationError.TooLong;
        }

        return name.All(IsValidCharacter)
            ? TopicNameValidationError.None
            : TopicNameValidationError.InvalidCharacters;
    }

    private static bool IsValidCharacter(char value)
        => char.IsAsciiLetterOrDigit(value) ||
           value is '.' or '_' or '-';
}
