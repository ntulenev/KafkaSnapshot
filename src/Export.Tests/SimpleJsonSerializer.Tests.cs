using FluentAssertions;

using Xunit;

using Moq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;
using System.Text;

namespace KafkaSnapshot.Export.Tests;

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
        var data = (IEnumerable<KeyValuePair<object, KafkaMessage<object>>>)null!;

        // Act
        var exception = Record.Exception(() => _ = serializer.Serialize(data, isRawData));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "SimpleJsonSerializer can't serialize null data to the stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void SimpleJsonSerializerCantSerializeNullDataToStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<SimpleJsonSerializer<object, object>>>().Object;
        var serializer = new SimpleJsonSerializer<object, object>(logger);
        var data = (IEnumerable<KeyValuePair<object, KafkaMessage<object>>>)null!;
        var stream = new Mock<Stream>(MockBehavior.Strict).Object;

        // Act
        var exception = Record.Exception(() => serializer.Serialize(data, isRawData, stream));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "SimpleJsonSerializer can't serialize null data to the null stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void SimpleJsonSerializerCantSerializeNullDataToNullStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<SimpleJsonSerializer<object, object>>>().Object;
        var serializer = new SimpleJsonSerializer<object, object>(logger);
        var data = (IEnumerable<KeyValuePair<object, KafkaMessage<object>>>)null!;

        // Act
        var exception = Record.Exception(() => serializer.Serialize(data, isRawData, null!));

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
            new KeyValuePair<object, KafkaMessage<object>>(1,new KafkaMessage<object>("Test",new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var result = serializer.Serialize(data, isRaw);

        // Assert
        result.Should().Be("[\r\n  {\r\n    \"Key\": 1,\r\n    \"Value\": {\r\n      \"Message\": \"Test\",\r\n      \"Meta\": {\r\n        \"Timestamp\": \"2020-12-12T01:02:03\",\r\n        \"Partition\": 1,\r\n        \"Offset\": 2\r\n      }\r\n    }\r\n  }\r\n]");
    }

    [Theory(DisplayName = "SimpleJsonSerializer can serialize data to the stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public void SimpleJsonSerializerCanSerializeDataToStream(bool isRaw)
    {
        // Arrange
        var logger = new Mock<ILogger<SimpleJsonSerializer<object, object>>>().Object;
        var serializer = new SimpleJsonSerializer<object, object>(logger);
        var dateTime = new DateTime(2020, 12, 12, 1, 2, 3);
        var data = new[]
        {
            new KeyValuePair<object, KafkaMessage<object>>(1,new KafkaMessage<object>("Test",new KafkaMetadata(dateTime,1,2)))
        };

        using var stream = new MemoryStream();

        // Act
        serializer.Serialize(data, isRaw, stream);

        // Assert
        var jsonString = Encoding.Default.GetString((stream.ToArray()));
        jsonString.Should().Be("[\r\n  {\r\n    \"Key\": 1,\r\n    \"Value\": {\r\n      \"Message\": \"Test\",\r\n      \"Meta\": {\r\n        \"Timestamp\": \"2020-12-12T01:02:03\",\r\n        \"Partition\": 1,\r\n        \"Offset\": 2\r\n      }\r\n    }\r\n  }\r\n]");
    }
}
