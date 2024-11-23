using FluentAssertions;

using Xunit;

using Moq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

using System.Text;

namespace KafkaSnapshot.Export.Tests;

public class OriginalKeySerializerTests
{
    [Fact(DisplayName = "OriginalKeySerializer cant be created without logger.")]
    [Trait("Category", "Unit")]
    public void OriginalKeySerializerCantBeCreated()
    {
        // Arrange
        var logger = (ILogger<OriginalKeySerializer<object>>)null!;

        // Act
        var exception = Record.Exception(() => _ = new OriginalKeySerializer<object>(logger));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "OriginalKeySerializer could be created.")]
    [Trait("Category", "Unit")]
    public void OriginalKeySerializerCouldBeCreated()
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;

        // Act
        var exception = Record.Exception(() => _ = new OriginalKeySerializer<object>(logger));

        // Assert
        exception.Should().BeNull();
    }

    [Theory(DisplayName = "OriginalKeySerializer can't serialize null data.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void OriginalKeySerializerCantSerializeNullData(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var data = (IEnumerable<KeyValuePair<object, KafkaMessage<string>>>)null!;

        // Act
        var exception = Record.Exception(() => _ = serializer.Serialize(data, isRawData));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "OriginalKeySerializer can't serialize null data to the stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void OriginalKeySerializerCantSerializeNullDataToStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var data = (IEnumerable<KeyValuePair<object, KafkaMessage<string>>>)null!;
        var stream = new Mock<Stream>(MockBehavior.Strict).Object;

        // Act
        var exception = Record.Exception(() => serializer.Serialize(data, isRawData, stream));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "OriginalKeySerializer can't serialize data to the null stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void OriginalKeySerializerCantSerializeDataToNullStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var data = new[]
        {
            new KeyValuePair<object, KafkaMessage<string>>(1,
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var exception = Record.Exception(() => serializer.Serialize(data, isRawData, null!));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "OriginalKeySerializer can serialize data.")]
    [Trait("Category", "Unit")]
    public void OriginalKeySerializerCanSerializeData()
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<object, KafkaMessage<string>>(1,
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var result = serializer.Serialize(data, isRaw);

        // Assert
        result.Should().Be("[\r\n  {\r\n    \"Key\": 1,\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "OriginalKeySerializer can serialize data to the stream.")]
    [Trait("Category", "Unit")]
    public void OriginalKeySerializerCanSerializeDataToTheStream()
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<object, KafkaMessage<string>>(1,
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };

        using var stream = new MemoryStream();

        // Act
        serializer.Serialize(data, isRaw, stream);

        // Assert
        var jsonString = Encoding.Default.GetString((stream.ToArray()));
        jsonString.Should().Be("[\r\n  {\r\n    \"Key\": 1,\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "OriginalKeySerializer cant serialize non json data.")]
    [Trait("Category", "Unit")]
    public void OriginalKeySerializerCantSerializeNonData()
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<object, KafkaMessage<string>>(1,
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };
        string result = null!;

        // Act
        var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<Newtonsoft.Json.JsonReaderException>();
    }

    [Fact(DisplayName = "OriginalKeySerializer cant serialize non json data to the stream.")]
    [Trait("Category", "Unit")]
    public void OriginalKeySerializerCantSerializeNonDataToStream()
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<object, KafkaMessage<string>>(1,
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };
        using var stream = new MemoryStream();

        // Act
        var exception = Record.Exception(() => serializer.Serialize(data, isRaw, stream));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<Newtonsoft.Json.JsonReaderException>();
    }

    [Fact(DisplayName = "OriginalKeySerializer can serialize raw data.")]
    [Trait("Category", "Unit")]
    public void OriginalKeySerializerCanSerializeRawData()
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = true;
        var data = new[]
        {
            new KeyValuePair<object, KafkaMessage<string>>(1,
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var result = serializer.Serialize(data, isRaw);

        // Assert
        result.Should().Be("[\r\n  {\r\n    \"Key\": 1,\r\n    \"Value\": \"Test\",\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "OriginalKeySerializer can serialize raw data to the stream.")]
    [Trait("Category", "Unit")]
    public void OriginalKeySerializerCanSerializeRawDataToStream()
    {
        // Arrange
        var logger = new Mock<ILogger<OriginalKeySerializer<object>>>().Object;
        var serializer = new OriginalKeySerializer<object>(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var isRaw = true;
        var data = new[]
        {
            new KeyValuePair<object, KafkaMessage<string>>(1,
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };
        using var stream = new MemoryStream();

        // Act
        serializer.Serialize(data, isRaw, stream);

        // Assert
        var jsonString = Encoding.Default.GetString((stream.ToArray()));
        jsonString.Should().Be("[\r\n  {\r\n    \"Key\": 1,\r\n    \"Value\": \"Test\",\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }
}
