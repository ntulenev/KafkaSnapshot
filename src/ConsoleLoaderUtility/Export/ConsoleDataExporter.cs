using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleLoaderUtility.Export
{
    public class ConsoleDataExporter : IDataExporter<string, string>
    {
        public Task ExportAsync(IDictionary<string, string> data, CancellationToken ct)
        {
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
