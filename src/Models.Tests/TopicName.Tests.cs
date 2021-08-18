using System;

using Xunit;

using FluentAssertions;

using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Models.Tests
{
    public class TopicNameTests
    {
        [Fact(DisplayName = "Topic name can't be null.")]
        [Trait("Category", "Unit")]
        public void CantCreateNullTopicName()
        {

            // Arrange
            string name = null!;

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Fact(DisplayName = "Topic name can't be empty.")]
        [Trait("Category", "Unit")]
        public void CantCreateEmptyTopicName()
        {

            // Arrange
            var name = string.Empty;

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name can't be whitespaces.")]
        [Trait("Category", "Unit")]
        public void CantCreateWhitespacesTopicName()
        {

            // Arrange
            var name = "     ";

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name can't have any whitespaces.")]
        [Trait("Category", "Unit")]
        public void CantCreateAnyWhitespacesTopicName()
        {

            // Arrange
            var name = "topic name";

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name can't have long names.")]
        [Trait("Category", "Unit")]
        public void CantCreateLongTopicName()
        {

            // Arrange
            var name = new string('x', 250);

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name can't have bad symbols names.")]
        [Trait("Category", "Unit")]
        public void CantCreateBadTopicName()
        {

            // Arrange
            var name = "ы?:%";

            // Act
            var exception = Record.Exception(() => new TopicName(name));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Fact(DisplayName = "Topic name equals other topic with same value.")]
        [Trait("Category", "Unit")]
        public void TopicNameEqualsSameTopic()
        {

            // Arrange
            var t1 = new TopicName("Topic1");
            var t2 = new TopicName("Topic1");
            var result = false;

            // Act
            var exception = Record.Exception(() =>
            {
                result = t1.Equals(t2);
            });

            // Assert
            exception.Should().BeNull();
            result.Should().BeTrue();
        }

        [Fact(DisplayName = "Topic name not equals other topic with other value.")]
        [Trait("Category", "Unit")]
        public void TopicNameNotEqualsOtherTopic()
        {

            // Arrange
            var t1 = new TopicName("Topic1");
            var t2 = new TopicName("Topic2");
            var result = false;

            // Act
            var exception = Record.Exception(() =>
            {
                result = t1.Equals(t2);
            });

            // Assert
            exception.Should().BeNull();
            result.Should().BeFalse();
        }

        [Fact(DisplayName = "Topic name has same hash with other topic with same value.")]
        [Trait("Category", "Unit")]
        public void TopicNameHasSameHashWithSameTopic()
        {

            // Arrange
            var t1 = new TopicName("Topic1");
            var t2 = new TopicName("Topic1");
            var result = false;

            // Act
            var exception = Record.Exception(() =>
            {
                result = t1.GetHashCode() == t2.GetHashCode();
            });

            // Assert
            exception.Should().BeNull();
            result.Should().BeTrue();
        }

        [Fact(DisplayName = "Topic name has different hash with other topic with other value.")]
        [Trait("Category", "Unit")]
        public void TopicNameHasOtherHashWithOtherTopic()
        {

            // Arrange
            var t1 = new TopicName("Topic1");
            var t2 = new TopicName("Topic2");
            var result = false;

            // Act
            var exception = Record.Exception(() =>
            {
                result = t1.GetHashCode() == t2.GetHashCode();
            });

            // Assert
            exception.Should().BeNull();
            result.Should().BeFalse();
        }
    }
}
