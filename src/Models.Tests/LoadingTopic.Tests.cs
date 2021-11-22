using Xunit;

using FluentAssertions;

using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Models.Tests
{
    public class LoadingTopicTests
    {
        [Theory(DisplayName = "Topic name can't be null.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateNullTopicName(bool compactingRule)
        {

            // Arrange
            string name = null!;

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "Topic name can't be null 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateNullTopicName2(bool compactingRule)
        {

            // Arrange
            string name = null!;

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, DateTime.UtcNow));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
        }

        [Theory(DisplayName = "Topic name can't be empty.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateEmptyTopicName(bool compactingRule)
        {

            // Arrange
            var name = string.Empty;

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't be empty 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateEmptyTopicName2(bool compactingRule)
        {

            // Arrange
            var name = string.Empty;

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, DateTime.UtcNow));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't be whitespaces.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateWhitespacesTopicName(bool compactingRule)
        {

            // Arrange
            var name = "     ";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't be whitespaces 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateWhitespacesTopicName2(bool compactingRule)
        {

            // Arrange
            var name = "     ";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, DateTime.UtcNow));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have any whitespaces.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateAnyWhitespacesTopicName(bool compactingRule)
        {

            // Arrange
            var name = "topic name";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have any whitespaces 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateAnyWhitespacesTopicName2(bool compactingRule)
        {

            // Arrange
            var name = "topic name";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, DateTime.UtcNow));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have long names.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateLongTopicName(bool compactingRule)
        {

            // Arrange
            var name = new string('x', 250);

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have long names 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateLongTopicName2(bool compactingRule)
        {

            // Arrange
            var name = new string('x', 250);

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, DateTime.UtcNow));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have bad symbols names.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateBadTopicName(bool compactingRule)
        {

            // Arrange
            var name = "ы?:%";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic name can't have bad symbols names 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CantCreateBadTopicName2(bool compactingRule)
        {

            // Arrange
            var name = "ы?:%";

            // Act
            var exception = Record.Exception(() => new LoadingTopic(name, compactingRule, DateTime.UtcNow));

            // Assert
            exception.Should().NotBeNull().And.BeOfType<ArgumentException>();
        }

        [Theory(DisplayName = "Topic with valid name can be created.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CanCreateValidTopic(bool compactingRule)
        {

            // Arrange
            var name = "test";
            LoadingTopic item = null!;
            // Act
            var exception = Record.Exception(() => item = new LoadingTopic(name, compactingRule));

            // Assert
            exception.Should().BeNull();
            item.Should().NotBeNull();
            item.LoadWithCompacting.Should().Be(compactingRule);
            item.HasOffsetDate.Should().BeFalse();
        }

        [Theory(DisplayName = "Topic with valid name can be created 2.")]
        [Trait("Category", "Unit")]
        [InlineData(true)]
        [InlineData(false)]
        public void CanCreateValidTopic2(bool compactingRule)
        {

            // Arrange
            var name = "test";
            LoadingTopic item = null!;
            // Act
            var exception = Record.Exception(() => item = new LoadingTopic(name, compactingRule, DateTime.UtcNow));

            // Assert
            exception.Should().BeNull();
            item.Should().NotBeNull();
            item.LoadWithCompacting.Should().Be(compactingRule);
            item.HasOffsetDate.Should().BeTrue();
        }

        [Fact(DisplayName = "Cant get topic offset date if not set.")]
        [Trait("Category", "Unit")]
        public void CanGetTopicOffsetDate()
        {

            // Arrange
            var name = "test";
            var topic = new LoadingTopic(name, true);

            // Act
            var exception = Record.Exception(() => topic.OffsetDate);

            // Assert
            exception.Should().NotBeNull().And.BeOfType<InvalidOperationException>();
        }

        [Fact(DisplayName = "Can get topic offset date if date is set.")]
        [Trait("Category", "Unit")]
        public void CantGetTopicOffsetDate()
        {

            // Arrange
            var date = DateTime.UtcNow;
            var name = "test";
            var topic = new LoadingTopic(name, true, date);
            DateTime resultedDate = default;

            // Act
            var exception = Record.Exception(() => resultedDate = topic.OffsetDate);

            // Assert
            exception.Should().BeNull();
            resultedDate.Should().Be(date);
        }
    }
}
