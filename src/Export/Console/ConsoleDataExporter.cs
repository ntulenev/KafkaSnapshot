using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Export
{
    public class ConsoleDataExporter : IDataExporter<string, string>
    {
        public Task ExportAsync(IDictionary<string, string> data, string topic, CancellationToken ct)
        {
            Console.WriteLine($"Topic : {topic}");
            foreach (var item in data)
            {
                Console.WriteLine(item.Key);
                Console.WriteLine(item.Value);
                Console.WriteLine("******************");
            }

            return Task.CompletedTask;
        }
    }
}
