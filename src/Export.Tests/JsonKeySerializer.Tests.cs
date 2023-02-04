using FluentAssertions;

using Xunit;

using Moq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Tests;

public class JsonKeySerializerTests
{
    [Fact(DisplayName = "JsonKeySerializer cant be created without logger.")]
    [Trait("Category", "Unit")]
    public void JsonKeySerializerCanBeCreated()
    {
        //Arrange
        var logger = (ILogger<JsonKeySerializer>)null!;

        // Act
        var exception = Record.Exception(() => _ = new JsonKeySerializer(logger));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "JsonKeySerializer could be created.")]
    [Trait("Category", "Unit")]
    public void JsonKeySerializerCouldBeCreated()
    {
        //Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;

        // Act
        var exception = Record.Exception(() => _ = new JsonKeySerializer(logger));

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
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var data = (IEnumerable<KeyValuePair<string, KafkaMessage<string>>>)null!;

        // Act
        var exception = Record.Exception(() => _ = serializer.Serialize(data, isRawData));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "JsonKeySerializer can't serialize null data to the stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void JsonKeySerializerCantSerializeNullDataToStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var data = (IEnumerable<KeyValuePair<string, KafkaMessage<string>>>)null!;
        var stream = new Mock<Stream>().Object;

        // Act
        var exception = Record.Exception(() => serializer.Serialize(data, isRawData, stream));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "JsonKeySerializer can't serialize data to the null stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void JsonKeySerializerCantSerializeDataToNullStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var data = new[]
                {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",new KafkaMessage<string>("{\"Test\":42}",new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var exception = Record.Exception(() => serializer.Serialize(data, isRawData, null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "JsonKeySerializer can serialize data.")]
    [Trait("Category", "Unit")]
    public void JsonKeySerializerCanSerializeData()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",new KafkaMessage<string>("{\"Test\":42}",new KafkaMetadata(dateTime,1,2)))
        };
        string result = null!;

        // Act
        var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

        // Assert
        exception.Should().BeNull();
        result.Should().Be("[\r\n  {\r\n    \"Key\": {\r\n      \"A\": 42\r\n    },\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "JsonKeySerializer cant serialize non json data.")]
    [Trait("Category", "Unit")]
    public void JsonKeySerializerCantSerializeNonJsonData()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",new KafkaMessage<string>("Test",new KafkaMetadata(dateTime,1,2)))
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
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("Test",new KafkaMessage<string>("{\"Test\":42}",new KafkaMetadata(dateTime,1,2)))
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
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = true;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",new KafkaMessage<string>("Test",new KafkaMetadata(dateTime,1,2)))
        };
        string result = null!;

        // Act
        var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

        // Assert
        exception.Should().BeNull();
        result.Should().Be("[\r\n  {\r\n    \"Key\": {\r\n      \"A\": 42\r\n    },\r\n    \"Value\": \"Test\",\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }
}
