﻿using System.Threading;
using System.Threading.Tasks;

using Models.Processing;

namespace Abstractions.Processing
{
    public interface IProcessingUnit
    {
        public Task ProcessAsync(CancellationToken ct);

        public ProcessingTopic Topic { get; }
    }
}
