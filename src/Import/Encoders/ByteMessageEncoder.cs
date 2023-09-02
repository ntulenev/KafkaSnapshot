using System.Data;
using System.Text;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Import;

using MessagePack;

namespace KafkaSnapshot.Import.Encoders
{
    public class ByteMessageEncoder : IMessageEncoder<byte[], string>
    {
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
                default: throw new NotSupportedException();
            }
        }
    }
}
