using ConsoleLoaderUtility.Tool.Configuration;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleLoaderUtility.Tool
{
    public interface IProcessingUnit
    {
        public Task ProcessAsync(CancellationToken ct);

        public LoadedTopic Topic { get; }
    }
}
