using FluentAssertions;

using Xunit;

using Moq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Tests
{
    public class SimpleJsonSerializerTests
    {
        [Fact(DisplayName = "SimpleJsonSerializer cant be created without logger.")]
        [Trait("Category", "Unit")]
        public void SimpleJsonSerializerCantBeCreated()
        {
            // Arrange
            var logger = (ILogger<SimpleJsonSerializer<object, object>>)null!;

            // Act
            var exception = Record.Exception(() => _ = new SimpleJsonSerializer<object, object>(logger));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "SimpleJsonSerializer could be created.")]
        [Trait("Category", "Unit")]
        public void SimpleJsonSerializerCouldBeCreated()
        {
            // Arrange
            var logger = new Mock<ILogger<SimpleJsonSerializer<object, object>>>().Object;

            // Act
            var exception = Record.Exception(() => _ = new SimpleJsonSerializer<object, object>(logger));

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
            var logger = new Mock<ILogger<SimpleJsonSerializer<object, object>>>().Object;
            var serializer = new SimpleJsonSerializer<object, object>(logger);
            var data = (IEnumerable<KeyValuePair<object, MetaMessage<object>>>)null!;

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
            var logger = new Mock<ILogger<SimpleJsonSerializer<object, object>>>().Object;
            var serializer = new SimpleJsonSerializer<object, object>(logger);
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var data = new[]
            {
                new KeyValuePair<object, MetaMessage<object>>(1,new MetaMessage<object>("Test",new KafkaMetadata(dateTime,1,2)))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().BeNull();
            result.Should().Be("[\r\n  {\r\n    \"Key\": 1,\r\n    \"Value\": {\r\n      \"Message\": \"Test\",\r\n      \"Meta\": {\r\n        \"Timestamp\": \"2020-12-12T01:02:03\",\r\n        \"Partition\": 1,\r\n        \"Offset\": 2\r\n      }\r\n    }\r\n  }\r\n]");
        }
    }
}
