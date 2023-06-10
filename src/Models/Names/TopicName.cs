namespace KafkaSnapshot.Models.Names;

/// <summary>
/// Represents a Kafka topic name.
/// </summary>
public class TopicName
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TopicName"/> class with the specified name.
    /// </summary>
    /// <param name="name">The name of the Kafka topic.</param>
    /// <exception cref="ArgumentNullException">Thrown if the topic name is null</exception>
    /// <exception cref="ArgumentException">Thrown if the topic name is empty, or has only spaces, 
    /// or name is tool longer, or contains invalid characters.</exception>
    public TopicName(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Topic name cannot be null or empty", nameof(name));
        }

        if (name.Length > MAX_TOPIC_NAME_LENGTH)
        {
            throw new ArgumentException(
                $"Topic name cannot be longer than {MAX_TOPIC_NAME_LENGTH} characters", 
                nameof(name));
        }

        if (!IsValidTopicName(name))
        {
            throw new ArgumentException("Topic name contains invalid characters", nameof(name));
        }

        _name = name;
    }

    /// <summary>
    /// Gets the name of the Kafka topic.
    /// </summary>
    public string Name => _name;

    private static bool IsValidTopicName(string name)
    {
        foreach (char c in name)
        {
            if (!char.IsLetterOrDigit(c) && c != '-' && c != '_' && c != '.')
            {
                return false;
            }
        }

        return true;
    }

    private const int MAX_TOPIC_NAME_LENGTH = 255;
    private readonly string _name;
}
