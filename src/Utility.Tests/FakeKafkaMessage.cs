namespace KafkaSnapshot.Utility.Tests;

internal sealed record FakeKafkaMessage<TKey>(
    TKey Key,
    string Value,
    long Offset,
    DateTime Timestamp,
    int Partition = 0)
    where TKey : notnull
{
    public StoredKafkaMessage ToStoredMessage()
        => new(Key, Value, Offset, Timestamp, Partition);
}

internal sealed record StoredKafkaMessage(
    object Key,
    string Value,
    long Offset,
    DateTime Timestamp,
    int Partition);

