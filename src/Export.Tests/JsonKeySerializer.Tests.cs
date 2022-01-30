using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

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

        [Theory(DisplayName = "JsonKeySerializer can't serialize null data.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void JsonKeySerializerCantSerializeNullData(bool isRawData)
        {
            // Arrange
            var serializer = new JsonKeySerializer();
            var data = (IEnumerable<KeyValuePair<string, DatedMessage<string>>>)null!;

            // Act
            var exception = Record.Exception(() => _ = serializer.Serialize(data, isRawData));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "JsonKeySerializer can serialize data.")]
        [Trait("Category", "Unit")]
        public void JsonKeySerializerCanSerializeData()
        {
            // Arrange
            var serializer = new JsonKeySerializer();
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = false;
            var data = new[]
            {
                new KeyValuePair<string, DatedMessage<string>>("{\"A\": 42}",new DatedMessage<string>("{\"Test\":42}",dateTime))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().BeNull();
            result.Should().Be("[\r\n  {\r\n    \"Key\": {\r\n      \"A\": 42\r\n    },\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Timestamp\": \"2020-12-12T01:02:03\"\r\n  }\r\n]");
        }

        [Fact(DisplayName = "JsonKeySerializer cant serialize non json data.")]
        [Trait("Category", "Unit")]
        public void JsonKeySerializerCantSerializeNonJsonData()
        {
            // Arrange
            var serializer = new JsonKeySerializer();
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = false;
            var data = new[]
            {
                new KeyValuePair<string, DatedMessage<string>>("{\"A\": 42}",new DatedMessage<string>("Test",dateTime))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<Newtonsoft.Json.JsonReaderException>();
        }

        [Fact(DisplayName = "JsonKeySerializer cant serialize non json key.")]
        [Trait("Category", "Unit")]
        public void JsonKeySerializerCantSerializeNonJsonKey()
        {
            // Arrange
            var serializer = new JsonKeySerializer();
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = false;
            var data = new[]
            {
                new KeyValuePair<string, DatedMessage<string>>("Test",new DatedMessage<string>("{\"Test\":42}",dateTime))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<Newtonsoft.Json.JsonReaderException>();
        }

        [Fact(DisplayName = "JsonKeySerializer can serialize raw data.")]
        [Trait("Category", "Unit")]
        public void JsonKeySerializerCanSerializeRawData()
        {
            // Arrange
            var serializer = new JsonKeySerializer();
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = true;
            var data = new[]
            {
                new KeyValuePair<string, DatedMessage<string>>("{\"A\": 42}",new DatedMessage<string>("Test",dateTime))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().BeNull();
            result.Should().Be("[\r\n  {\r\n    \"Key\": {\r\n      \"A\": 42\r\n    },\r\n    \"Value\": \"Test\",\r\n    \"Timestamp\": \"2020-12-12T01:02:03\"\r\n  }\r\n]");
        }
    }
}
