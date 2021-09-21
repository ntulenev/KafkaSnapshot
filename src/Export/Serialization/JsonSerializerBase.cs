using System.Diagnostics;
using System.IO;
using System.Text;

using Newtonsoft.Json;

namespace KafkaSnapshot.Export.Serialization
{
    /// <summary>
    /// Base class for Json serialization
    /// </summary>
    public abstract class JsonSerializerBase
    {
        protected string SerializeData(object data)
        {
            Debug.Assert(data is not null);

            var sb = new StringBuilder();

            using var textWriter = new StringWriter(sb);

            using var jsonWriter = new JsonTextWriter(textWriter);

            _serializer.Serialize(jsonWriter, data);

            return sb.ToString();
        }

        private readonly JsonSerializer _serializer = new() { Formatting = Formatting.Indented };
    }
}
