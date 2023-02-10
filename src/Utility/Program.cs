using Microsoft.Extensions.Hosting;

using KafkaSnapshot.Utility;

var host = HostBuildHelper.CreateHost();
await host.RunAsync();
