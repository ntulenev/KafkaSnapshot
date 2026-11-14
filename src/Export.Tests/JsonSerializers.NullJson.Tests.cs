using FluentAssertions;

using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace KafkaSnapshot.Export.Tests;

public class JsonSerializersNullJsonTests
{
    [Theory(DisplayName = "JSON serializers reject a JSON null payload in parsed mode for both string and stream output.")]
    [Trait("Category", "Unit")]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task RejectsJsonNullPayload(int serializerType)
    {
        var data = new[] { new KeyValuePair<string, KafkaMessage<string>>("{}",
            new KafkaMessage<string>("null", new KafkaMetadata(DateTime.UnixEpoch, 0, 0))) };
        using var stream = new MemoryStream();
        var ignore = new IgnoreKeySerializer(NullLogger<IgnoreKeySerializer>.Instance);
        var json = new JsonKeySerializer(NullLogger<JsonKeySerializer>.Instance);
        var original = new OriginalKeySerializer<string>(NullLogger<OriginalKeySerializer<string>>.Instance);

        Action sync = () => _ = serializerType switch
        {
            0 => ignore.Serialize(data, false),
            1 => json.Serialize(data, false),
            _ => original.Serialize(data, false)
        };
        Func<Task> async = () => serializerType switch
        {
            0 => ignore.SerializeAsync(data, false, stream, CancellationToken.None),
            1 => json.SerializeAsync(data, false, stream, CancellationToken.None),
            _ => original.SerializeAsync(data, false, stream, CancellationToken.None)
        };

        sync.Should().Throw<InvalidOperationException>().WithMessage("JSON value cannot be null.");
        await async.Should().ThrowAsync<InvalidOperationException>().WithMessage("JSON value cannot be null.");
    }
}
