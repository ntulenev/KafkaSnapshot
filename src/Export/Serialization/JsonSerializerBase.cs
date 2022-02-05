using System.Diagnostics;
using System.Text;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

namespace KafkaSnapshot.Export.Serialization
{
    /// <summary>
    /// Base class for Json serialization
    /// </summary>
    public abstract class JsonSerializerBase
    {
        /// <summary>
        /// Creates <see cref="JsonSerializerBase"/>.
        /// </summary>
        /// <param name="logger">Logger.</param>
        public JsonSerializerBase(ILogger<JsonSerializerBase> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        protected string SerializeData(object data)
        {
            Debug.Assert(data is not null);

            var sb = new StringBuilder();

            using var textWriter = new StringWriter(sb);

            using var jsonWriter = new JsonTextWriter(textWriter);

            _logger.LogTrace("Start serializing data.");

            _serializer.Serialize(jsonWriter, data);

            _logger.LogTrace("Finish serializing data.");

            return sb.ToString();
        }

        private readonly JsonSerializer _serializer = new() { Formatting = Formatting.Indented };

        private readonly ILogger<JsonSerializerBase> _logger;
    }
}
