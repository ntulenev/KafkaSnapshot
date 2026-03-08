using KafkaSnapshot.Abstractions.Export;
using KafkaSnapshot.Export.File.Common;
using KafkaSnapshot.Export.File.Output;
using KafkaSnapshot.Export.Markers;
using KafkaSnapshot.Export.Serialization;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KafkaSnapshot.Utility.Helpers;

/// <summary>
/// Registers export services.
/// </summary>
internal static class ExportRegistrationHelper
{
    internal static void Register(IServiceCollection services, HostBuilderContext hostContext)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(hostContext);

        _ = services.ConfigureExport(hostContext);
        _ = services.AddSingleton<ISerializer<string, string, IgnoreKeyMarker>, IgnoreKeySerializer>();
        _ = services.AddSingleton<ISerializer<string, string, JsonKeyMarker>, JsonKeySerializer>();
        _ = services.AddSingleton<ISerializer<string, string, OriginalKeyMarker>, OriginalKeySerializer<string>>();
        _ = services.AddSingleton<ISerializer<long, string, OriginalKeyMarker>, OriginalKeySerializer<long>>();
        _ = services.AddSingleton(typeof(IDataExporter<,,,>), typeof(JsonFileDataExporter<,,,>));
        _ = services.AddSingleton<IFileSaver, FileSaver>();
        _ = services.AddSingleton<IFileStreamProvider, FileStreamProvider>();
    }
}
