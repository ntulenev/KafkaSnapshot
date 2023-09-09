using KafkaSnapshot.Models.Import;

namespace KafkaSnapshot.Abstractions.Import
{

    /// <summary>
    /// Interface for encoding input messages into output messages based 
    /// on specified rules.
    /// </summary>
    /// <typeparam name="TMessageIn">The type of input message.</typeparam>
    /// <typeparam name="TMessageOut">The type of output message.</typeparam>
    public interface IMessageEncoder<TMessageIn, TMessageOut>
    {
        /// <summary>
        /// Encodes the given input message based on the specified encoding rule.
        /// </summary>
        /// <param name="message">The input message to be encoded.</param>
        /// <param name="rule">The encoding rule to be applied.</param>
        /// <returns>The encoded output message.</returns>
        TMessageOut Encode(TMessageIn message, EncoderRules rule);
    }
}
