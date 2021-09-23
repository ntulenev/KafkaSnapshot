using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.Serialization;

namespace KafkaSnapshot.Export.Tests
{
    public class SimpleJsonSerializerTests
    {
        [Fact(DisplayName = "SimpleJsonSerializer could be created.")]
        [Trait("Category", "Unit")]
        public void SimpleJsonSerializerCouldBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new SimpleJsonSerializer<object,object>());

            // Assert
            exception.Should().BeNull();
        }
    }
}
