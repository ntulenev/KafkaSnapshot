using System.Threading;
using System.Threading.Tasks;

namespace ConsoleLoaderUtility.Tool
{
    public interface ITopicProcessor
    {
        public Task ProcessTopicAsync(string name, CancellationToken ct);
    }
}
