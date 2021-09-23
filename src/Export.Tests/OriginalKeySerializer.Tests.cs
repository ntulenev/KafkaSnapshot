using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.Serialization;

namespace KafkaSnapshot.Export.Tests
{
    public class OriginalKeySerializerTests
    {
        [Fact(DisplayName = "OriginalKeySerializer could be created.")]
        [Trait("Category", "Unit")]
        public void OriginalKeySerializerCouldBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new OriginalKeySerializer<object>());

            // Assert
            exception.Should().BeNull();
        }
    }
}
