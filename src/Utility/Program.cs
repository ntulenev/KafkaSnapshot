using KafkaSnapshot.Utility.DependencyInjection;

using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);
_ = builder.Services.AddKafkaSnapshotUtility(builder.Configuration);

using var host = builder.Build();
await host.RunAsync().ConfigureAwait(false);
