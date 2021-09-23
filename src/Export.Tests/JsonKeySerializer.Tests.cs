using System.Collections.Generic;
using System;

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
    }
}
