using System.Diagnostics;
using System.Globalization;

using Confluent.Kafka;

if (args.Length != 2 || !int.TryParse(args[1], NumberStyles.None, CultureInfo.InvariantCulture, out var seconds) || seconds <= 0)
{
    Console.Error.WriteLine("Usage: KafkaProbe <bootstrap-servers> <timeout-seconds>");
    return 2;
}

using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = args[0] }).Build();
var elapsed = Stopwatch.StartNew();
Exception? lastError = null;
while (elapsed.Elapsed < TimeSpan.FromSeconds(seconds))
{
    var remaining = TimeSpan.FromSeconds(seconds) - elapsed.Elapsed;
    if (remaining <= TimeSpan.Zero)
    {
        break;
    }

    try
    {
        var metadata = admin.GetMetadata(remaining < TimeSpan.FromSeconds(2) ? remaining : TimeSpan.FromSeconds(2));
        if (metadata.Brokers.Count > 0)
        {
            Console.WriteLine($"Kafka metadata is available at {args[0]} ({metadata.Brokers.Count} brokers).");
            return 0;
        }
    }
    catch (KafkaException exception)
    {
        lastError = exception;
    }

    await Task.Delay(100);
}

Console.Error.WriteLine($"Kafka metadata did not become available within {seconds} seconds at {args[0]}. {lastError?.Message}");
return 1;
