using FluentAssertions;

using KafkaSnapshot.Import.Encoders;
using KafkaSnapshot.Models.Import;

using MessagePack;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class ByteMessageEncoderTests
{
    public static IEnumerable<object[]> TestData()
    {
        var strData1 = new string('A', 100);
        var strData2 = new string('B', 100);
        var testObject = new ByteMessageTestType
        {
            Field1 = strData1,
            Field2 = strData2
        };

        var result = "[\"" + strData1 + "\",0,\"" + strData2 + "\"]";

        var parametersNone = MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.None);
        byte[] serializedDataNone = MessagePackSerializer.Serialize(testObject, parametersNone);

        var parametersLZ4 = MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4Block);
        byte[] serializedDataLZ4 = MessagePackSerializer.Serialize(testObject, parametersNone);

        yield return new object[] { new byte[] { 72, 101, 108, 108, 111 }, EncoderRules.String, "Hello" };
        yield return new object[] { serializedDataNone, EncoderRules.MessagePack, result };
        yield return new object[] { serializedDataLZ4, EncoderRules.MessagePackLz4Block, result };
    }

    [Trait("Category", "Unit")]
    [Theory(DisplayName = "Encode should convert byte array to string based on specified rules")]
    [MemberData(nameof(TestData))]
    public void EncodeConvertsByteArrayToString(byte[] inputBytes, EncoderRules rule, string expectedOutput)
    {
        // Arrange
        var encoder = new ByteMessageEncoder();

        // Act
        var result = encoder.Encode(inputBytes, rule);

        // Assert
        result.Should().Be(expectedOutput);
    }

    [Trait("Category", "Unit")]
    [Fact(DisplayName = "Encode should throw ArgumentNullException when input message is null")]
    public void EncodeNullMessageThrowsArgumentNullException()
    {
        // Arrange
        var encoder = new ByteMessageEncoder();
        byte[] message = null!;
        EncoderRules rule = EncoderRules.String;

        // Act
        Action act = () => encoder.Encode(message, rule);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Trait("Category", "Unit")]
    [Fact(DisplayName = "Encode should throw ArgumentException when invalid EncoderRules value is provided")]
    public void EncodeInvalidEncoderRuleThrowsArgumentException()
    {
        // Arrange
        var encoder = new ByteMessageEncoder();
        byte[] message = Array.Empty<byte>();
        EncoderRules rule = (EncoderRules)99;

        // Act
        Action act = () => encoder.Encode(message, rule);

        // Assert
        act.Should().Throw<ArgumentException>();
    }
}
