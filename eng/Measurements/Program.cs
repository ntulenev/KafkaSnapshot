using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.Configuration;
using KafkaSnapshot.Export.File.Output;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Export.Serialization;
using KafkaSnapshot.Import;
using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Import.Encoders;
using KafkaSnapshot.Import.Kafka;
using KafkaSnapshot.Import.Metadata;
using KafkaSnapshot.Models.Filters;
using KafkaSnapshot.Models.Import;
using KafkaSnapshot.Models.Names;
using KafkaSnapshot.Models.Sorting;
using KafkaSnapshot.Sorting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

var mode = args[0];
var bootstrap = args[1];
var topicName = args[2];
if (mode == "prepare")
{
    var count = int.Parse(args[3], System.Globalization.CultureInfo.InvariantCulture);
    using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
    await admin.CreateTopicsAsync([new TopicSpecification { Name = topicName, NumPartitions = 1, ReplicationFactor = 1 }]);
    using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
    {
        BootstrapServers = bootstrap,
        LingerMs = 5,
        BatchSize = 1024 * 1024,
        MessageTimeoutMs = 30000
    }).Build();
    for (var offset = 0; offset < count; offset += 1000)
    {
        var tasks = Enumerable.Range(offset, Math.Min(1000, count - offset)).Select(i =>
            producer.ProduceAsync(topicName, new Message<string, byte[]>
            {
                Key = i.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Value = Encoding.UTF8.GetBytes(new string('x', 1000) + i.ToString("D10", System.Globalization.CultureInfo.InvariantCulture))
            }));
        await Task.WhenAll(tasks);
    }
    Console.WriteLine(JsonSerializer.Serialize(new { mode, count }));
    return;
}
if (mode == "delete")
{
    using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build();
    await admin.DeleteTopicsAsync([topicName]);
    return;
}
var output = args[3];
var config = Options.Create(new SnapshotLoaderConfiguration
{
    DateOffsetTimeout = TimeSpan.FromSeconds(5),
    MaxConcurrentPartitions = 1,
    StreamBufferCapacity = 64
});
var factory = new KafkaClientFactory(Options.Create(new BootstrapServersConfiguration { BootstrapServers = [bootstrap] }));
var reader = new PartitionSnapshotReader<string, string>(NullLogger<PartitionSnapshotReader<string, string>>.Instance,
    config, factory.CreateConsumer<string>, new ByteMessageEncoder(), new PartitionConsumerAssignment());
var batch = new PartitionSnapshotBatchReader<string, string>(config, reader);
var builder = new SnapshotBuilder<string, string>(NullLogger<SnapshotBuilder<string, string>>.Instance,
    new MessageSorter<string, string>(new SortingParams(SortingType.Time, SortingOrder.None)));
var loader = new SnapshotLoader<string, string>(NullLogger<SnapshotLoader<string, string>>.Instance,
    new TopicWatermarkLoader(factory, Options.Create(new TopicWatermarkLoaderConfiguration { AdminClientTimeout = TimeSpan.FromSeconds(10) })), batch, builder);
var topic = new LoadingTopic(new TopicName(topicName), false, new DateFilterRange(null, null), EncoderRules.String, null);
var filter = new AcceptAll();
GC.Collect();
GC.WaitForPendingFinalizers();
GC.Collect();
var stopwatch = Stopwatch.StartNew();
long firstWriteTicks = 0;
long peakManaged = 0;
using var process = Process.GetCurrentProcess();
using var samples = new Timer(_ =>
{
    var current = GC.GetTotalMemory(false);
    long old;
    do { old = Volatile.Read(ref peakManaged); }
    while (current > old && Interlocked.CompareExchange(ref peakManaged, current, old) != old);
}, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(10));
var storage = new MeasuredStorage(output, () => Interlocked.CompareExchange(ref firstWriteTicks, stopwatch.ElapsedTicks, 0));
var exporter = new JsonFileDataExporter<string, OriginalKeyMarker, string>(
    Options.Create(new JsonFileDataExporterConfiguration { UseFileStreaming = true }),
    NullLogger<JsonFileDataExporter<string, OriginalKeyMarker, string>>.Instance, storage,
    new OriginalKeySerializer<string>(NullLogger<OriginalKeySerializer<string>>.Instance));
using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
var exported = new KafkaSnapshot.Models.Export.ExportedTopic(new TopicName(topicName), new FileName(output), true);
if (mode == "buffered")
{
    var snapshot = await loader.LoadSnapshotAsync(topic, filter, filter, cts.Token);
    await exporter.ExportAsync(snapshot, exported, cts.Token);
}
else
{
    await exporter.ExportAsync(loader.StreamSnapshotAsync(topic, filter, filter, cts.Token), exported, cts.Token);
}
stopwatch.Stop();
await samples.DisposeAsync();
var peakWorkingMiB = process.PeakWorkingSet64 / 1048576d;
long exportedMessages = 0;
await using (var verification = File.OpenRead(output))
{
    await foreach (var entry in JsonSerializer.DeserializeAsyncEnumerable<JsonElement>(verification))
    {
        if (entry.GetProperty("Meta").GetProperty("Offset").GetInt64() != exportedMessages)
        {
            throw new InvalidOperationException("Output offsets are missing or out of order.");
        }

        exportedMessages++;
    }
}
Console.WriteLine(JsonSerializer.Serialize(new
{
    mode,
    exportedMessages,
    elapsedMs = stopwatch.Elapsed.TotalMilliseconds,
    firstWriteMs = firstWriteTicks * 1000d / Stopwatch.Frequency,
    peakManagedMiB = peakManaged / 1048576d,
    peakWorkingMiB,
    outputBytes = new FileInfo(output).Length,
    framework = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
    os = System.Runtime.InteropServices.RuntimeInformation.OSDescription
}));

sealed class AcceptAll : KafkaSnapshot.Abstractions.Filters.IDataFilter<string>
{
    public bool IsMatch(string data) => true;
}
sealed class MeasuredStorage(string path, Action onWrite) : IExportFileStorage
{
    public Task SaveAsync(FileName fileName, string content, CancellationToken ct) => throw new NotSupportedException();
    public Stream CreateFileStream(FileName fileName) => new MeasuredStream(
        new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read, 65536, FileOptions.Asynchronous | FileOptions.SequentialScan), onWrite);
}
sealed class MeasuredStream(Stream inner, Action onWrite) : Stream
{
    public override bool CanRead => inner.CanRead;
    public override bool CanSeek => inner.CanSeek;
    public override bool CanWrite => inner.CanWrite;
    public override long Length => inner.Length;
    public override long Position { get => inner.Position; set => inner.Position = value; }
    public override void Flush() => inner.Flush();
    public override Task FlushAsync(CancellationToken ct) => inner.FlushAsync(ct);
    public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);
    public override void SetLength(long value) => inner.SetLength(value);
    public override void Write(byte[] buffer, int offset, int count) { onWrite(); inner.Write(buffer, offset, count); }
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) { onWrite(); return inner.WriteAsync(buffer, offset, count, ct); }
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) { onWrite(); return inner.WriteAsync(buffer, ct); }
    protected override void Dispose(bool disposing) { if (disposing) inner.Dispose(); base.Dispose(disposing); }
    public override async ValueTask DisposeAsync() { await inner.DisposeAsync(); GC.SuppressFinalize(this); }
}
