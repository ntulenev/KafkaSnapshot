using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Abstractions.Import
{
    public interface IMessageEncoder<TMessageIn, TMessageOut>
    {
        public TMessageOut Encode(TMessageIn message, EncoderRules rule);
    }
}
