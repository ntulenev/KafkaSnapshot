using System;
using System.Diagnostics;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using KafkaSnapshot.Import.Configuration;
using KafkaSnapshot.Processing.Configuration;

namespace KafkaSnapshot.Utility
{
    /// <summary>
    /// Helpers for common config data.
    /// </summary>
    public static class ConfigHelpers
    {
        /// <summary>
        /// Gets configuration for Kafka servers.
        /// </summary>
        public static BootstrapServersConfiguration GetBootstrapConfig(this IServiceProvider sp, IConfiguration configuration)
        {
            var section = configuration.GetSection(nameof(BootstrapServersConfiguration));
            var config = section.Get<BootstrapServersConfiguration>();

            Debug.Assert(config is not null);

            var validator = sp.GetRequiredService<IValidateOptions<BootstrapServersConfiguration>>();

            // Crutch to use IValidateOptions in manual generation logic.
            var validationResult = validator.Validate(string.Empty, config);
            if (validationResult.Failed)
            {
                throw new OptionsValidationException
                    (string.Empty, typeof(BootstrapServersConfiguration), new[] { validationResult.FailureMessage });
            }

            return config;
        }

        /// <summary>
        /// Gets configuration for Kafka topics.
        /// </summary>
        public static LoaderToolConfiguration GetLoaderConfig(this IServiceProvider sp, IConfiguration configuration)
        {
            var section = configuration.GetSection(nameof(LoaderToolConfiguration));
            var config = section.Get<LoaderToolConfiguration>();

            Debug.Assert(config is not null);

            var validator = sp.GetRequiredService<IValidateOptions<LoaderToolConfiguration>>();

            // Crutch to use IValidateOptions in manual generation logic.
            var validationResult = validator.Validate(string.Empty, config);
            if (validationResult.Failed)
            {
                throw new OptionsValidationException
                    (string.Empty, typeof(LoaderToolConfiguration), new[] { validationResult.FailureMessage });
            }

            return config;
        }
    }
}
