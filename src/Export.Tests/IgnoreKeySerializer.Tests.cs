using FluentAssertions;

using Xunit;

using Moq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Tests
{
    public class IgnoreKeySerializerTests
    {
        [Fact(DisplayName = "IgnoreKeySerializer can't be created without logger.")]
        [Trait("Category", "Unit")]
        public void IgnoreKeySerializerCantBeCreated()
        {
            // Arrange
            var logger = (ILogger<IgnoreKeySerializer>)null!;

            // Act
            var exception = Record.Exception(() => _ = new IgnoreKeySerializer(logger));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "IgnoreKeySerializer could be created.")]
        [Trait("Category", "Unit")]
        public void IgnoreKeySerializerCouldBeCreated()
        {
            // Arrange
            var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;

            // Act
            var exception = Record.Exception(() => _ = new IgnoreKeySerializer(logger));

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
            var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
            var serializer = new IgnoreKeySerializer(logger);
            var data = (IEnumerable<KeyValuePair<string, KafkaMessage<string>>>)null!;

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
            var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
            var serializer = new IgnoreKeySerializer(logger);
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = false;
            var data = new[]
            {
                new KeyValuePair<string, KafkaMessage<string>>(null!,new KafkaMessage<string>("{\"Test\":42}",new KafkaMetadata(dateTime,1,2)))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().BeNull();
            result.Should().Be("[\r\n  {\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
        }

        [Fact(DisplayName = "IgnoreKeySerializer cant serialize non json data.")]
        [Trait("Category", "Unit")]
        public void IgnoreKeySerializerCantSerializeNonJsonData()
        {
            // Arrange
            var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
            var serializer = new IgnoreKeySerializer(logger);
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = false;
            var data = new[]
            {
                new KeyValuePair<string, KafkaMessage<string>>(null!,new KafkaMessage<string>("Test",new KafkaMetadata(dateTime,1,2)))
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
            var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
            var serializer = new IgnoreKeySerializer(logger);
            var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
            var isRaw = true;
            var data = new[]
            {
                new KeyValuePair<string, KafkaMessage<string>>(null!,new KafkaMessage<string>("Test",new KafkaMetadata(dateTime,1,2)))
            };
            string result = null!;

            // Act
            var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

            // Assert
            exception.Should().BeNull();
            result.Should().Be("[\r\n  {\r\n    \"Value\": \"Test\",\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
        }
    }
}
