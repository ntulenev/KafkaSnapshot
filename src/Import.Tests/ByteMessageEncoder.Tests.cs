using FluentAssertions;

using KafkaSnapshot.Import.Encoders;

using KafkaSnapshot.Models.Import;

using Xunit;

namespace KafkaSnapshot.Import.Tests;

public class ByteMessageEncoderTests
{
    [Trait("Category", "Unit")]
    [Theory(DisplayName = "Encode should convert byte array to string based on specified rules")]
    [InlineData(new byte[] { 72, 101, 108, 108, 111 }, EncoderRules.String, "Hello")]
    [InlineData(new byte[] { 147, 164, 74, 111, 104, 110, 192, 30 }, EncoderRules.MessagePack, "[\"John\",null,30]")]
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
