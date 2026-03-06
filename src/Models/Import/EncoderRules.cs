namespace KafkaSnapshot.Models.Import;

/// <summary>
/// Specifies the rules for encoding messages.
/// </summary>
#pragma warning disable CA1720
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
    MessagePackLz4Block,

    /// <summary>
    /// Represents encoding the message using Base64 format.
    /// </summary>
    Base64
}
#pragma warning restore CA1720
