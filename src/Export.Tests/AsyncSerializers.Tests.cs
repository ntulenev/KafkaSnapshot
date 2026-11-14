using System.Text;
using System.Text.Json.Nodes;

using FluentAssertions;

using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Models.Message;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace KafkaSnapshot.Export.Tests;

[Trait("Category", "Unit")]
public class AsyncSerializersTests
{
    [Theory(DisplayName = "All async serializers preserve the synchronous JSON format.")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncFormatsMatchExistingExports(bool raw)
    {
        await Verify(new OriginalKeySerializer<string>(NullLogger<OriginalKeySerializer<string>>.Instance), raw);
        await Verify(new JsonKeySerializer(NullLogger<JsonKeySerializer>.Instance), raw);
        await Verify(new IgnoreKeySerializer(NullLogger<IgnoreKeySerializer>.Instance), raw);
        await Verify(new SimpleJsonSerializer<string, string>(NullLogger<SimpleJsonSerializer<string, string>>.Instance), raw);
    }

    private static async Task Verify<TMarker>(ISerializer<string, string, TMarker> serializer, bool raw)
        where TMarker : IKeyRepresentationMarker
    {
        KeyValuePair<string, KafkaMessage<string>>[] items =
        [
            new(/*lang=json,strict*/ "{\"id\":1}", new(/*lang=json,strict*/ "{\"value\":1}", new(DateTimeOffset.UnixEpoch, 0, 0))),
            new(/*lang=json,strict*/ "{\"id\":2}", new(/*lang=json,strict*/ "{\"value\":2}", new(DateTimeOffset.UnixEpoch, 1, 0)))
        ];
        var expected = JsonNode.Parse(serializer.Serialize(items, raw));
        await using var stream = new MemoryStream();
        await serializer.SerializeAsync(Source(items), raw, stream, CancellationToken.None);
        JsonNode.DeepEquals(expected, JsonNode.Parse(Encoding.UTF8.GetString(stream.ToArray()))).Should().BeTrue();
    }

    private static async IAsyncEnumerable<T> Source<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }
}
