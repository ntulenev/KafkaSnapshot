namespace KafkaSnapshot.Models.Names;

/// <summary>Failure categories for a Kafka topic name.</summary>
public enum TopicNameValidationError
{
    /// <summary>The name is valid.</summary>
    None,
    /// <summary>The name was not provided.</summary>
    Missing,
    /// <summary>The name is empty or contains only whitespace.</summary>
    Empty,
    /// <summary>The name contains whitespace.</summary>
    Whitespace,
    /// <summary>The name exceeds the supported length.</summary>
    TooLong,
    /// <summary>The name contains unsupported characters.</summary>
    InvalidCharacters
}
