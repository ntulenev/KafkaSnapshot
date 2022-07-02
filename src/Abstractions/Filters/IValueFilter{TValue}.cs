namespace KafkaSnapshot.Abstractions.Filters
{

    /// <summary>
    /// Value filter for loading data from Kafka.
    /// </summary>
    /// <typeparam name="TKey">Message key type.</typeparam>
    internal interface IValueFilter<TValue> : IDataFilter<TValue> where TValue : notnull
    {
    }
}
