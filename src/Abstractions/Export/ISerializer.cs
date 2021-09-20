namespace KafkaSnapshot.Abstractions.Export
{
    /// <summary>
    /// Export data serializer
    /// </summary>
    public interface ISerializer
    {
        /// <summary>
        /// Serializes data in string
        /// </summary>
        /// <param name="value">Data for serialization</param>
        /// <returns>String data representation</returns>
        public string Serialize(object value);
    }
}
