using Xunit;

using Microsoft.Extensions.Options;


using FluentAssertions;

using KafkaSnapshot.Models.Configuration;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;

namespace KafkaSnapshot.Processing.Tests;

public class LoaderToolConfigurationValidatorTests
{
    [Fact(DisplayName = "LoaderToolConfigurationValidator can be created.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCanBeCreated()
    {

        // Arrange

        // Act
        var exception = Record.Exception(() => new LoaderToolConfigurationValidator());

        // Assert
        exception.Should().BeNull();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator accepts valid topic settings.")]
    [Trait("Category", "Unit")]
    public void ValidateAcceptsValidTopics()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>
             {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
             }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator accepts an ordered date range.")]
    [Trait("Category", "Unit")]
    public void ValidateAcceptsOrderedDateRange()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var startDate = new DateTimeOffset(2022, 1, 1, 10, 0, 0, TimeSpan.Zero);
        var endDate = startDate.AddHours(1);
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = false,
            Topics =
            [
                new TopicConfiguration
                {
                    Name = "test",
                    Compacting = CompactingMode.Off,
                    ExportFileName = "export",
                    FilterKeyType = Models.Filters.FilterType.None,
                    KeyType = Models.Filters.KeyType.String,
                    OffsetStartDate = startDate,
                    OffsetEndDate = endDate
                }
            ]
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate("Test", options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeTrue();
    }


    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects duplicate export file names.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsDuplicateExportFileNames()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>
             {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "Export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },

             }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.ExportFileNameDuplicate);
        result.Failures.Should().ContainSingle()
            .Which.Should().StartWith(
                ConfigurationValidationErrorCodes.ExportFileNameDuplicate);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator can't be validated with null options.")]
    [Trait("Category", "Unit")]
    public void LoaderToolConfigurationValidatorCannotBeValidatedWithNullOptions()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        var options = (LoaderToolConfiguration)null!;

        // Act
        var exception = Record.Exception(() => validator.Validate(name, options));

        // Assert
        exception.Should().NotBeNull().And.BeOfType<ArgumentNullException>();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects a missing topics collection.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsMissingTopics()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = null!
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicsMissing);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects an empty topics collection.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsEmptyTopics()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicsEmpty);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects a null topic name.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsMissingTopicName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = null!,
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicNameMissing);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects an empty topic name.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsEmptyTopicName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = string.Empty,
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicNameEmpty);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects a whitespace-only topic name.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsWhitespaceOnlyTopicName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "   ",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicNameEmpty);
    }

    [Theory(DisplayName = "LoaderToolConfigurationValidator rejects whitespace inside a topic name.")]
    [Trait("Category", "Unit")]
    [InlineData(" test")]
    [InlineData("test ")]
    [InlineData("te st")]
    public void ValidateRejectsWhitespaceInsideTopicName(string topicName)
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = topicName,
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicNameWhitespace);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects a topic name longer than 249 characters.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsTopicNameOver249Characters()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = new string('a',250),
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicNameTooLong);
    }


    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects unsupported characters in a topic name.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsUnsupportedTopicNameCharacters()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "*()=%$#@!!",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicNameInvalid);
    }


    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects a null export file name.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsMissingExportFileName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = null!,
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.ExportFileNameMissing);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects an empty export file name.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsEmptyExportFileName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = string.Empty,
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.ExportFileNameEmpty);
    }


    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects a whitespace-only export file name.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsWhitespaceOnlyExportFileName()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "  ",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.None,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.ExportFileNameEmpty);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator accepts a key filter with a configured value.")]
    [Trait("Category", "Unit")]
    public void ValidateAcceptsConfiguredKeyFilter()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = "{}",
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Succeeded.Should().BeTrue();
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects a missing value for an enabled key filter.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsMissingFilterValue()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = "{}",
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = null!,
                      KeyType = Models.Filters.KeyType.Json,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.FilterValueMissing);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects compaction when keys are ignored.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsCompactionWithIgnoredKeys()
    {

        // Arrange
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            UseConcurrentLoad = true,
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = "{}",
                      KeyType = Models.Filters.KeyType.Json,
                  },
                  new TopicConfiguration
                  {
                      Name = "test",
                      Compacting = CompactingMode.On,
                      ExportFileName = "export",
                      ExportRawMessage = true,
                      FilterKeyType = Models.Filters.FilterType.Equals,
                      FilterKeyValue = "{}",
                      KeyType = Models.Filters.KeyType.Ignored,
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.CompactingIgnoredKey);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects a start date after the end date.")]
    [Trait("Category", "Unit")]
    public void ValidateRejectsReversedDateRange()
    {

        // Arrange
        var startDate = new DateTimeOffset(2022, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var endDate = startDate.AddDays(-1);
        var validator = new LoaderToolConfigurationValidator();
        var name = "Test";
        ValidateOptionsResult result = null!;
        var options = new LoaderToolConfiguration
        {
            Topics = new List<TopicConfiguration>()
            {
                  new TopicConfiguration
                  {
                      Name = "test",
                      ExportFileName = "export",
                      KeyType = Models.Filters.KeyType.Json,
                      OffsetStartDate = startDate,
                      OffsetEndDate = endDate
                  }
            }
        };

        // Act
        var exception = Record.Exception(() => result = validator.Validate(name, options));

        // Assert
        exception.Should().BeNull();
        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.TopicDateRangeInvalid);
    }
}

