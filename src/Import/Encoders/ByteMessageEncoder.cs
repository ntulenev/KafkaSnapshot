using System.Text;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Import;

using MessagePack;

namespace KafkaSnapshot.Import.Encoders
{

    /// <summary>
    /// Encoder for converting byte array messages to string format based on specified rules.
    /// </summary>
    public class ByteMessageEncoder : IMessageEncoder<byte[], string>
    {

        /// <inheritdoc>/>
        /// <exception cref="ArgumentNullException">Thrown when the input message is null.</exception>
        /// <exception cref="ArgumentException">Thrown when an invalid EncoderRules value is provided.</exception>
        /// <exception cref="NotSupportedException">Thrown when an unsupported encoding rule is provided.</exception>
        public string Encode(byte[] message, EncoderRules rule)
        {
            ArgumentNullException.ThrowIfNull(message);

            if (!Enum.IsDefined(typeof(EncoderRules), rule))
            {
                throw new ArgumentException($"Invalid EncoderRules value {rule}", nameof(rule));
            }

            switch (rule)
            {
                case EncoderRules.String:
                    {
                        return Encoding.UTF8.GetString(message);
                    }
                case EncoderRules.MessagePack:
                    {
                        return MessagePackSerializer.ConvertToJson(message);
                    }
                case EncoderRules.MessagePackLz4Block:
                    {
                        return MessagePackSerializer.ConvertToJson(
                            message,
                            MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4Block));
                    }

                default: throw new NotSupportedException($"Unsupported encoding rule: {rule}");

            }
        }
    }
}
