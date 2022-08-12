using KafkaSnapshot.Models.Sorting;

namespace KafkaSnapshot.Processing.Configuration
{
    /// <summary>
    /// Application configuration.
    /// </summary>
    public class LoaderToolConfiguration
    {
        /// <summary>
        /// List of  topics with string Key.
        /// </summary>
        public List<TopicConfiguration> Topics { get; set; } = default!;

        /// <summary>
        /// User <see cref="LoaderConcurrentTool"/> to process topics in concurrent mode.
        /// </summary>
        public bool UseConcurrentLoad { get; set; }

        /// <summary>
        /// Message sorting field.
        /// </summary>
        public SortingType GlobalSortingValue => SortingType.Time;

        /// <summary>
        /// Message soring type.
        /// </summary>
        public SortingOrder GlobalSrotingOrder => SortingOrder.No;
    }
}
