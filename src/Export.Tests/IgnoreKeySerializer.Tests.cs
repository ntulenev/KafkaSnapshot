using FluentAssertions;

using Xunit;

using Moq;

using Microsoft.Extensions.Logging;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

using System.Text;

namespace KafkaSnapshot.Export.Tests;

public class IgnoreKeySerializerTests
{
    [Fact(DisplayName = "IgnoreKeySerializer can't be created without logger.")]
    [Trait("Category", "Unit")]
    public void IgnoreKeySerializerCannotBeCreated()
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
    public void IgnoreKeySerializerCannotSerializeNullData(bool isRawData)
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

    [Theory(DisplayName = "IgnoreKeySerializer can't serialize null data to the stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task IgnoreKeySerializerCannotSerializeNullDataToStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
        var serializer = new IgnoreKeySerializer(logger);
        var data = (IEnumerable<KeyValuePair<string, KafkaMessage<string>>>)null!;
        var stream = new Mock<Stream>(MockBehavior.Strict).Object;

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRawData, stream, CancellationToken.None)).ConfigureAwait(false);

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Theory(DisplayName = "IgnoreKeySerializer can't serialize data to the null stream.")]
    [Trait("Category", "Unit")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task IgnoreKeySerializerCannotSerializeDataToNullStream(bool isRawData)
    {
        // Arrange
        var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
        var serializer = new IgnoreKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>(null!,
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRawData, null!, CancellationToken.None)).ConfigureAwait(false);

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
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>(null!,
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var result = serializer.Serialize(data, isRaw);

        // Assert
        result.Should().Be("[\r\n  {\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03+00:00\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "IgnoreKeySerializer can serialize data to stream.")]
    [Trait("Category", "Unit")]
    public async Task IgnoreKeySerializerCanSerializeDataToStream()
    {
        // Arrange
        var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
        var serializer = new IgnoreKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>(null!,
                new KafkaMessage<string>("{\"Test\":42}",
                    new KafkaMetadata(dateTime,1,2)))
        };
        using var stream = new MemoryStream();

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRaw, stream, CancellationToken.None)).ConfigureAwait(false);

        // Assert
        var jsonString = Encoding.Default.GetString((stream.ToArray()));
        jsonString.Should().Be("[\r\n  {\r\n    \"Value\": {\r\n      \"Test\": 42\r\n    },\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03+00:00\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "IgnoreKeySerializer can't serialize non json data.")]
    [Trait("Category", "Unit")]
    public void IgnoreKeySerializerCannotSerializeNonJsonData()
    {
        // Arrange
        var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
        var serializer = new IgnoreKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>(null!,
                new KafkaMessage<string>("Test",new KafkaMetadata(dateTime,1,2)))
        };
        string result = null!;

        // Act
        var exception = Record.Exception(() => result = serializer.Serialize(data, isRaw));

        // Assert
        exception.Should().NotBeNull().And.BeAssignableTo<System.Text.Json.JsonException>();
    }

    [Fact(DisplayName = "IgnoreKeySerializer can't serialize non json data to the stream.")]
    [Trait("Category", "Unit")]
    public async Task IgnoreKeySerializerCannotSerializeNonJsonDataToTheStream()
    {
        // Arrange
        var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
        var serializer = new IgnoreKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = false;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>(null!,
                new KafkaMessage<string>("Test",new KafkaMetadata(dateTime,1,2)))
        };
        var stream = new MemoryStream();

        // Act
        var exception = await Record.ExceptionAsync(() => serializer.SerializeAsync(data, isRaw, stream, CancellationToken.None)).ConfigureAwait(false);

        // Assert
        exception.Should().NotBeNull().And.BeAssignableTo<System.Text.Json.JsonException>();
    }

    [Fact(DisplayName = "IgnoreKeySerializer can serialize raw data.")]
    [Trait("Category", "Unit")]
    public void IgnoreKeySerializerCanSerializeRawData()
    {
        // Arrange
        var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
        var serializer = new IgnoreKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = true;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>(null!,
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var result = serializer.Serialize(data, isRaw);

        // Assert
        result.Should().Be("[\r\n  {\r\n    \"Value\": \"Test\",\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03+00:00\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "IgnoreKeySerializer can serialize raw data to the stream.")]
    [Trait("Category", "Unit")]
    public async Task IgnoreKeySerializerCanSerializeRawDataToStream()
    {
        // Arrange
        var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
        var serializer = new IgnoreKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = true;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>(null!,
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };
        var stream = new MemoryStream();

        // Act
        await serializer.SerializeAsync(data, isRaw, stream, CancellationToken.None).ConfigureAwait(false);

        // Assert
        var jsonString = Encoding.Default.GetString((stream.ToArray()));
        jsonString.Should().Be("[\r\n  {\r\n    \"Value\": \"Test\",\r\n    \"Meta\": {\r\n      \"Timestamp\": \"2020-12-12T01:02:03+00:00\",\r\n      \"Partition\": 1,\r\n      \"Offset\": 2\r\n    }\r\n  }\r\n]");
    }

    [Fact(DisplayName = "IgnoreKeySerializer can serialize data with non null keys by ignoring them.")]
    [Trait("Category", "Unit")]
    public void IgnoreKeySerializerCanSerializeDataWithNotNullKeys()
    {
        // Arrange
        var logger = new Mock<ILogger<IgnoreKeySerializer>>().Object;
        var serializer = new IgnoreKeySerializer(logger);
        var dateTime = new DateTimeOffset(2020, 12, 12, 1, 2, 3, TimeSpan.Zero);
        var isRaw = true;
        var data = new[]
        {
            new KeyValuePair<string, KafkaMessage<string>>("test-key",
                new KafkaMessage<string>("Test",
                    new KafkaMetadata(dateTime,1,2)))
        };

        // Act
        var result = serializer.Serialize(data, isRaw);

        // Assert
        result.Should().Contain("\"Value\": \"Test\"");
        result.Should().NotContain("test-key");
    }
}
