using System.Collections.Generic;
using System;

using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

namespace KafkaSnapshot.Export.Tests
{
    public class OriginalKeySerializerTests
    {
        [Fact(DisplayName = "OriginalKeySerializer could be created.")]
        [Trait("Category", "Unit")]
        public void OriginalKeySerializerCouldBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new OriginalKeySerializer<object>());

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
            var serializer = new OriginalKeySerializer<object>();
            var data = (IEnumerable<KeyValuePair<object, DatedMessage<string>>>)null!;

            // Act
            var exception = Record.Exception(() => _ = serializer.Serialize(data, isRawData));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }
    }
}
