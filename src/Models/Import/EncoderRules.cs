namespace KafkaSnapshot.Models.Import
{
    /// <summary>
    /// Specifies the rules for encoding messages.
    /// </summary>
    public enum EncoderRules
    {
        /// <summary>
        /// Represents encoding the message as a string.
        /// </summary>
        String,

        /// <summary>
        /// Represents encoding the message using MessagePack format.
        /// </summary>
        MessagePack,

        /// <summary>
        /// Represents encoding the message using MessagePack format with LZ4 block compression.
        /// </summary>
        MessagePackLz4Block
    }
}
