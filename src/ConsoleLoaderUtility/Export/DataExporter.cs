using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleLoaderUtility.Export
{
    public class DataExporter : IDataExporter<string>
    {
        public Task ExportAsync(IEnumerable<string> data, CancellationToken ct)
        {
            foreach (var item in data)
            {
                Console.WriteLine(data);
            }

            return Task.CompletedTask;
        }
    }
}
