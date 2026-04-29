namespace KafkaSnapshot.Models.Names;

/// <summary>
/// Represents a Kafka topic name.
/// </summary>
public sealed class TopicName
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TopicName"/> class with the specified name.
    /// </summary>
    /// <param name="name">The name of the Kafka topic.</param>
    /// <exception cref="ArgumentNullException">Thrown if the topic name is null.</exception>
    /// <exception cref="ArgumentException">Thrown if the topic name is empty, whitespace,
    /// too long, or contains invalid characters.</exception>
    public TopicName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (!KafkaTopicNameRules.IsValid(name))
        {
            throw new ArgumentException("Topic name contains invalid characters", nameof(name));
        }

        Name = name;
    }

    /// <summary>
    /// Gets the name of the Kafka topic.
    /// </summary>
    public string Name { get; }

}
