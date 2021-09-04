using System;

namespace KafkaSnapshot.Import.Configuration
{
    public class SnapshotLoaderConfiguration
    {
        /// <summary>
        /// Timout for searching date offset.
        /// </summary>
        public TimeSpan DateOffsetTimeout { get; set; }
    }
}
