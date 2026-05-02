using FluentAssertions;

using Xunit;

using Moq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

using System.Text;

namespace KafkaSnapshot.Export.Tests;

public class JsonKeySerializerTests
{
    [Fact(DisplayName = "JsonKeySerializer can't be created without logger.")]
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

    [Fact(DisplayName = "JsonKeySerializer can be created.")]
    [Trait("Category", "Unit")]
    public void JsonKeySerializerCannotBeCreatedWithoutLogger()
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
    public void JsonKeySerializerCannotSerializeNullData(bool isRawData)
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
    public async Task JsonKeySerializerCannotSerializeNullDataToStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var data = (IEnumerable<KeyValuePair<string, KafkaMessage<string>>>)null!;
        var stream = new Mock<Stream>(MockBehavior.Strict).Object;

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRawData, stream, CancellationToken.None)).ConfigureAwait(false);

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "JsonKeySerializer can't serialize data to the null stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task JsonKeySerializerCannotSerializeDataToNullStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var data = new[]
                {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRawData, null!, CancellationToken.None)).ConfigureAwait(false);

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
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };
        string result = null!;

        // Act
        var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

        // Assert
        exception.Should().BeNull();
        result.Should().Be("[\r\n  {\r\n    \"Key\": {\r\n      \"A\": 42\r\n    },\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03+00:00\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "JsonKeySerializer can serialize data to the stream.")]
    [Trait("Category", "Unit")]
    public async Task JsonKeySerializerCanSerializeDataToStream()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };
        using var stream = new MemoryStream();

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRaw, stream, CancellationToken.None)).ConfigureAwait(false);

        // Assert
        var jsonString = Encoding.Default.GetString((stream.ToArray()));
        jsonString.Should().Be("[\r\n  {\r\n    \"Key\": {\r\n      \"A\": 42\r\n    },\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03+00:00\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "JsonKeySerializer can't serialize non-JSON data.")]
    [Trait("Category", "Unit")]
    public void JsonKeySerializerCannotSerializeNonJsonData()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };
        string result = null!;

        // Act
        var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

        // Assert
        exception.Should().NotBeNull().And.BeAssignableTo<System.Text.Json.JsonException>();
    }

    [Fact(DisplayName = "JsonKeySerializer can't serialize non-JSON data to the stream.")]
    [Trait("Category", "Unit")]
    public async Task JsonKeySerializerCannotSerializeNonJsonDataToStream()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };
        var stream = new MemoryStream();

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRaw, stream, CancellationToken.None)).ConfigureAwait(false);

        // Assert
        exception.Should().NotBeNull().And.BeAssignableTo<System.Text.Json.JsonException>();
    }

    [Fact(DisplayName = "JsonKeySerializer can't serialize non-JSON key.")]
    [Trait("Category", "Unit")]
    public void JsonKeySerializerCannotSerializeNonJsonKey()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("Test",
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };
        string result = null!;

        // Act
        var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

        // Assert
        exception.Should().NotBeNull().And.BeAssignableTo<System.Text.Json.JsonException>();
    }

    [Fact(DisplayName = "JsonKeySerializer can't serialize non-JSON key to the stream.")]
    [Trait("Category", "Unit")]
    public async Task JsonKeySerializerCannotSerializeNonJsonKeyToStream()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("Test",
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };
        var stream = new MemoryStream();

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRaw, stream, CancellationToken.None)).ConfigureAwait(false);

        // Assert
        exception.Should().NotBeNull().And.BeAssignableTo<System.Text.Json.JsonException>();
    }

    [Fact(DisplayName = "JsonKeySerializer can serialize raw data.")]
    [Trait("Category", "Unit")]
    public void JsonKeySerializerCanSerializeRawData()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = true;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };
        string result = null!;

        // Act
        var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

        // Assert
        exception.Should().BeNull();
        result.Should().Be("[\r\n  {\r\n    \"Key\": {\r\n      \"A\": 42\r\n    },\r\n    \"Value\": \"Test\",\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03+00:00\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "JsonKeySerializer can serialize raw data to the stream.")]
    [Trait("Category", "Unit")]
    public async Task JsonKeySerializerCanSerializeRawDataToStream()
    {
        // Arrange
        var logger = new Mock<ILogger<JsonKeySerializer>>().Object;
        var serializer = new JsonKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = true;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("{\"A\": 42}",
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };
        var stream = new MemoryStream();

        // Act
        await serializer.SerializeAsync(data, isRaw, stream, CancellationToken.None).ConfigureAwait(false);

        // Assert
        var jsonString = Encoding.Default.GetString((stream.ToArray()));
        jsonString.Should().Be("[\r\n  {\r\n    \"Key\": {\r\n      \"A\": 42\r\n    },\r\n    \"Value\": \"Test\",\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03+00:00\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }
}
