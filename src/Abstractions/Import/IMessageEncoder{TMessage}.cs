namespace KafkaSnapshot.Abstractions.Import
{
    public interface IMessageEncoder<TMessage>
    {
        public TMessage Encode(TMessage message);
    }
}
