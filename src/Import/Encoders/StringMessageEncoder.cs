using System.Text;

using KafkaSnapshot.Abstractions.Import;
using KafkaSnapshot.Models.Import;

using MessagePack;

namespace KafkaSnapshot.Import.Encoders
{
    class StringMessageEncoder : IMessageEncoder<string>
    {
        public StringMessageEncoder(EncoderRules rule)
        {
            if (!Enum.IsDefined(typeof(EncoderRules), rule))
            {
                throw new ArgumentException($"Invalid EncoderRules value {rule}", nameof(rule));
            }

            _rule = rule;
        }

        public string Encode(string message)
        {
            ArgumentNullException.ThrowIfNull(message);

            switch (_rule)
            {
                case EncoderRules.Nothing: return message;
                case EncoderRules.MessagePack:
                    {
                        var bytes = Encoding.UTF8.GetBytes(message);
                        return MessagePackSerializer.ConvertToJson(bytes);
                    }
                default: throw new NotSupportedException();
            }
        }

        private readonly EncoderRules _rule;
    }
}
