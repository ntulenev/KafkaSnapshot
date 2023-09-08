using MessagePack;

namespace KafkaSnapshot.Import.Tests
{
    [MessagePackObject]
    public class ByteMessageTestType
    {
        [Key(0)]
        public string Field1 { get; set; } = default!;

        [Key(1)]
        public int FieldIgnored { get; set; }

        [Key(2)]
        public string Field2 { get; set; } = default!;

    }
}
