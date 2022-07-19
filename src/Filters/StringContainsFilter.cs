using KafkaSnapshot.Abstractions.Filters;

namespace KafkaSnapshot.Filters
{

    /// <summary>
    /// String contains filter.
    /// </summary>
    public class StringContainsFilter : IDataFilter<string>
    {

        /// <summary>
        /// Creates <see cref="StringContainsFilter"/>.
        /// </summary>
        /// <param name="sample">String should contains this pattern.</param>
        /// <exception cref="ArgumentNullException">If sample is null.</exception>
        /// <exception cref="ArgumentException">If sample is empty of contains only spaces.</exception>
        public StringContainsFilter(string sample)
        {
            ArgumentNullException.ThrowIfNull(sample);

            if (string.IsNullOrWhiteSpace(sample))
            {
                throw new ArgumentException("Sample is empty of whitespace.", nameof(sample));
            }

            _sample = sample;
        }

        /// <inheritdoc/>
        public bool IsMatch(string data)
        {
            ArgumentNullException.ThrowIfNull(data);

            return data.Contains(_sample);
        }

        private readonly string _sample;
    }
}
