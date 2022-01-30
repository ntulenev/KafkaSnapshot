using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Tests
{
    public class IgnoreKeySerializerTests
    {
        [Fact(DisplayName = "IgnoreKeySerializer could be created.")]
        [Trait("Category", "Unit")]
        public void IgnoreKeySerializerCouldBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new IgnoreKeySerializer());

            // Assert
            exception.Should().BeNull();
        }

        [Theory(DisplayName = "IgnoreKeySerializer can't serialize null data.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void IgnoreKeySerializerCantSerializeNullData(bool isRawData)
        {
            // Arrange
            var serializer = new IgnoreKeySerializer();
            var data = (IEnumerable<KeyValuePair<string, DatedMessage<string>>>)null!;

            // Act
            var exception = Record.Exception(() => _ = serializer.Serialize(data, isRawData));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "IgnoreKeySerializer can serialize data.")]
        [Trait("Category", "Unit")]
        public void IgnoreKeySerializerCanSerializeData()
        {
            // Arrange
            var serializer = new IgnoreKeySerializer();
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = false;
            var data = new[]
            {
                new KeyValuePair<string, DatedMessage<string>>(null!,new DatedMessage<string>("{\"Test\":42}",dateTime))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().BeNull();
            result.Should().Be("[\r\n  {\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Timestamp\": \"2020-12-12T01:02:03\"\r\n  }\r\n]");
        }

        [Fact(DisplayName = "IgnoreKeySerializer cant serialize non json data.")]
        [Trait("Category", "Unit")]
        public void IgnoreKeySerializerCantSerializeNonJsonData()
        {
            // Arrange
            var serializer = new IgnoreKeySerializer();
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = false;
            var data = new[]
            {
                new KeyValuePair<string, DatedMessage<string>>(null!,new DatedMessage<string>("Test",dateTime))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<Newtonsoft.Json.JsonReaderException>();
        }

        [Fact(DisplayName = "IgnoreKeySerializer can serialize raw data.")]
        [Trait("Category", "Unit")]
        public void IgnoreKeySerializerCanSerializeRawData()
        {
            // Arrange
            var serializer = new IgnoreKeySerializer();
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = true;
            var data = new[]
            {
                new KeyValuePair<string, DatedMessage<string>>(null!,new DatedMessage<string>("Test",dateTime))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().BeNull();
            result.Should().Be("[\r\n  {\r\n    \"Value\": \"Test\",\r\n    \"Timestamp\": \"2020-12-12T01:02:03\"\r\n  }\r\n]");
        }
    }
}
