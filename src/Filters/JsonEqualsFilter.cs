using Newtonsoft.Json.Linq;

using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters
{
    /// <summary>
    /// Json equals filter
    /// </summary>
    public class JsonEqualsFilter : IKeyFilter<string>
    {
        /// <summary>
        /// Creates <see cref="JsonEqualsFilter"/>.
        /// </summary>
        /// <param name="sample">Key sample.</param>
        public JsonEqualsFilter(string sample)
        {
            ArgumentNullException.ThrowIfNull(sample);

            _sample = JObject.Parse(sample);
        }

        /// <inheritdoc/>
        public bool IsMatch(string key)
        {
            ArgumentNullException.ThrowIfNull(key);

            var jsonKey = JObject.Parse(key);


            return JToken.DeepEquals(jsonKey, _sample);
        }

        private readonly JObject _sample;
    }
}
