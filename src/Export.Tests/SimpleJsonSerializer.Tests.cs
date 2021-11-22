using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Tests
{
    public class SimpleJsonSerializerTests
    {
        [Fact(DisplayName = "SimpleJsonSerializer could be created.")]
        [Trait("Category", "Unit")]
        public void SimpleJsonSerializerCouldBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new SimpleJsonSerializer<object, object>());

            // Assert
            exception.Should().BeNull();
        }

        [Theory(DisplayName = "SimpleJsonSerializer can't serialize null data.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void SimpleJsonSerializerCantSerializeNullData(bool isRawData)
        {
            // Arrange
            var serializer = new SimpleJsonSerializer<object, object>();
            var data = (IEnumerable<KeyValuePair<object, DatedMessage<object>>>)null!;

            // Act
            var exception = Record.Exception(() => _ = serializer.Serialize(data, isRawData));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "SimpleJsonSerializer can serialize data.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void SimpleJsonSerializerCanSerializeData(bool isRaw)
        {
            // Arrange
            var serializer = new SimpleJsonSerializer<object, object>();
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var data = new[]
            {
                new KeyValuePair<object, DatedMessage<object>>(1,new DatedMessage<object>("Test",dateTime))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().BeNull();
            result.Should().Be("[\r\n  {\r\n    \"Key\": 1,\r\n    \"Value\": {\r\n      \"Message\": \"Test\",\r\n      \"Timestamp\": \"2020-12-12T01:02:03\"\r\n    }\r\n  }\r\n]");
        }
    }
}
