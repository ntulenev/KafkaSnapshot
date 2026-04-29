namespace KafkaSnapshot.Models.Configuration;

#pragma warning disable CS1591

/// <summary>
/// Stable configuration validation error codes.
/// </summary>
public static class ConfigurationValidationErrorCodes
{
    public static string BootstrapServersMissing { get; } = "KS_CFG_001";
    public static string BootstrapServersEmpty { get; } = "KS_CFG_002";
    public static string BootstrapServersEmptyItem { get; } = "KS_CFG_003";
    public static string BootstrapServersWhitespaceItem { get; } = "KS_CFG_004";
    public static string SecurityProtocolUnsupported { get; } = "KS_CFG_005";
    public static string SaslMechanismUnsupported { get; } = "KS_CFG_006";
    public static string SaslMechanismMissing { get; } = "KS_CFG_007";
    public static string SaslUsernameMissing { get; } = "KS_CFG_008";
    public static string SaslPasswordMissing { get; } = "KS_CFG_009";
    public static string SaslProtocolMissing { get; } = "KS_CFG_010";
    public static string DateOffsetTimeoutInvalid { get; } = "KS_CFG_011";
    public static string MaxConcurrentPartitionsInvalid { get; } = "KS_CFG_012";
    public static string AdminClientTimeoutInvalid { get; } = "KS_CFG_013";
    public static string TopicsMissing { get; } = "KS_CFG_014";
    public static string TopicsEmpty { get; } = "KS_CFG_015";
    public static string TopicNameMissing { get; } = "KS_CFG_016";
    public static string TopicNameEmpty { get; } = "KS_CFG_017";
    public static string TopicNameWhitespace { get; } = "KS_CFG_018";
    public static string TopicNameTooLong { get; } = "KS_CFG_019";
    public static string TopicNameInvalid { get; } = "KS_CFG_020";
    public static string ExportFileNameMissing { get; } = "KS_CFG_021";
    public static string ExportFileNameEmpty { get; } = "KS_CFG_022";
    public static string FilterValueMissing { get; } = "KS_CFG_023";
    public static string CompactingIgnoredKey { get; } = "KS_CFG_024";
    public static string TopicDateRangeInvalid { get; } = "KS_CFG_025";
    public static string ExportFileNameDuplicate { get; } = "KS_CFG_026";
    public static string OutputDirectoryWhitespace { get; } = "KS_CFG_027";
    public static string KeyTypeUnsupported { get; } = "KS_CFG_028";
    public static string FilterTypeUnsupported { get; } = "KS_CFG_029";
    public static string CompactingModeUnsupported { get; } = "KS_CFG_030";
    public static string EncoderRuleUnsupported { get; } = "KS_CFG_031";
    public static string SortingTypeUnsupported { get; } = "KS_CFG_032";
    public static string SortingOrderUnsupported { get; } = "KS_CFG_033";
}

#pragma warning restore CS1591
