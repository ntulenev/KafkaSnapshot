using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaSnapshot.Import.Configuration
{
    public class SnapshotLoaderConfiguration
    {
        /// <summary>
        /// Timout for searching date offset.
        /// </summary>
        public TimeSpan DateOssetTimeout { get; set; }
    }
}
