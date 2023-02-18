namespace KafkaSnapshot.Models.Names;

public class TopicName
{
    public TopicName(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Topic name cannot be null or empty", nameof(name));
        }

        if (name.Length > MAX_TOPIC_NAME_LENGTH)
        {
            throw new ArgumentException($"Topic name cannot be longer than {MAX_TOPIC_NAME_LENGTH} characters", nameof(name));
        }

        if (!IsValidTopicName(name))
        {
            throw new ArgumentException("Topic name contains invalid characters", nameof(name));
        }

        _name = name;
    }

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
