using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleLoaderUtility.Export
{
    public class JsonFileDataExporter : IDataExporter<string, string>

    {
        public JsonFileDataExporter(string fileName)
        {
            _fileName = fileName;
        }

        public async Task ExportAsync(IDictionary<string, string> data, CancellationToken ct)
        {
            var inner = string.Join(",\n", data.Values);
            var sb = new StringBuilder();
            sb.AppendLine("{");
            sb.AppendLine("\"items\": [");
            sb.AppendLine(inner);
            sb.AppendLine("]");
            sb.AppendLine("}");
            await File.WriteAllTextAsync(_fileName, sb.ToString(), ct);
        }

        private readonly string _fileName;


    }
}
