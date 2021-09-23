using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.Serialization;

namespace KafkaSnapshot.Export.Tests
{
    public class JsonKeySerializerTests
    {
        [Fact(DisplayName = "JsonKeySerializer could be created.")]
        [Trait("Category", "Unit")]
        public void JsonKeySerializerCouldBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new JsonKeySerializer());

            // Assert
            exception.Should().BeNull();
        }
    }
}
