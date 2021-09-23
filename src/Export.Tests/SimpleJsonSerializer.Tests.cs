using FluentAssertions;

using Xunit;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;
using System.Collections.Generic;
using System;

namespace KafkaSnapshot.Export.Tests
{
    public class SimpleJsonSerializerTests
    {
        [Fact(DisplayName = "SimpleJsonSerializer could be created.")]
        [Trait("Category", "Unit")]
        public void SimpleJsonSerializerCouldBeCreated()
        {
            // Act
            var exception = Record.Exception(() => _ = new SimpleJsonSerializer<object,object>());

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
            var serializer = new SimpleJsonSerializer<object, object>();
            var data = (IEnumerable<KeyValuePair<object, DatedMessage<object>>>)null!;

            // Act
            var exception = Record.Exception(() => _ = serializer.Serialize(data, isRawData));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }
    }
}
