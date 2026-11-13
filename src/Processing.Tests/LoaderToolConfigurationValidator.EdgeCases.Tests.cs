using FluentAssertions;
using KafkaSnapshot.Models.Configuration;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Sorting;
using KafkaSnapshot.Processing.Configuration;
using KafkaSnapshot.Processing.Configuration.Validation;
using Xunit;

namespace KafkaSnapshot.Processing.Tests;

[Trait("Category", "Unit")]
public class LoaderToolConfigurationValidatorEdgeCasesTests
{
    [Theory(DisplayName = "LoaderToolConfigurationValidator reports the specific error for each unsupported enum value.")]
    [InlineData(0, "KS_CFG_029")]
    [InlineData(1, "KS_CFG_030")]
    [InlineData(2, "KS_CFG_031")]
    [InlineData(3, "KS_CFG_032")]
    [InlineData(4, "KS_CFG_033")]
    public void RejectsUnsupportedEnumsWithSpecificError(int invalidSetting, string code)
    {
        var options = new LoaderToolConfiguration
        {
            GlobalMessageSort = invalidSetting == 3 ? (SortingType)999 : SortingType.Time,
            GlobalSortOrder = invalidSetting == 4 ? (SortingOrder)999 : SortingOrder.None,
            Topics = [new TopicConfiguration
            {
                Name = "customers",
                ExportFileName = "customers.json",
                KeyType = KeyType.String,
                FilterKeyType = invalidSetting == 0 ? (FilterType)999 : FilterType.None,
                Compacting = invalidSetting == 1 ? (CompactingMode)999 : CompactingMode.Off,
                MessageEncoderRule = invalidSetting == 2 ? (EncoderRules)999 : EncoderRules.String
            }]
        };

        var result = new LoaderToolConfigurationValidator().Validate(null, options);

        result.Failed.Should().BeTrue();
        result.Failures.Should().ContainSingle().Which.Should().StartWith(code);
    }

    [Fact(DisplayName = "LoaderToolConfigurationValidator rejects null options.")]
    public void RejectsNullOptions()
    {
        Action act = () => new LoaderToolConfigurationValidator().Validate(null, null!);
        act.Should().Throw<ArgumentNullException>().WithParameterName("options");
    }

    [Theory(DisplayName = "LoaderToolConfigurationValidator rejects duplicate export names regardless of casing.")]
    [InlineData(false)]
    [InlineData(true)]
    public void RejectsDuplicateExportFilesIncludingCaseDifferences(bool differentCase)
    {
        var options = new LoaderToolConfiguration
        {
            Topics = [
                new TopicConfiguration { Name = "first", ExportFileName = "output.json", KeyType = KeyType.String },
                new TopicConfiguration { Name = "second", ExportFileName = differentCase ? "OUTPUT.json" : "output.json", KeyType = KeyType.String }
            ]
        };

        var result = new LoaderToolConfigurationValidator().Validate(null, options);

        result.Failures.Should().ContainSingle().Which.Should()
            .StartWith(ConfigurationValidationErrorCodes.ExportFileNameDuplicate);
    }
}
