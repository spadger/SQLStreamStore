namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;

    /// <summary>
    ///     Represents an implementation of <see cref="IStreamStoreNotifier"/> that polls
    ///     the target stream store for new message.
    /// </summary>
    public sealed class PollingBasedStoreNotifier : IStreamStoreNotifier
    {
        private readonly IReadonlyStreamStore _readonlyStreamStore;
#if NET461
        private static readonly ILog s_logger = LogProvider.GetCurrentClassLogger();
#elif NETSTANDARD1_3
        private static readonly ILog s_logger = LogProvider.GetLogger("SqlStreamStore.Subscriptions.PollingStreamStoreNotifier");
#endif
        private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
        private readonly Func<CancellationToken, Task<long>> _readHeadPosition;
        private readonly int _interval;
        private readonly TaskCompletionSource<Unit> _initialized = new TaskCompletionSource<Unit>();

        /// <summary>
        ///     Initializes a new instance of of <see cref="PollingBasedStoreNotifier"/>.
        /// </summary>
        /// <param name="readonlyStreamStore">The store to poll.</param>
        /// <param name="interval">The interval to poll in milliseconds. Default is 1000.</param>
        public PollingBasedStoreNotifier(IReadonlyStreamStore readonlyStreamStore, int interval = 1000)
            : this(readonlyStreamStore.ReadHeadPosition, interval)
        {
            _readonlyStreamStore = readonlyStreamStore;
        }

        /// <summary>
        ///     Initializes a new instance of of <see cref="PollingBasedStoreNotifier"/>.
        /// </summary>
        /// <param name="readHeadPosition">An operation to read the head position of a store.</param>
        /// <param name="interval">The interval to poll in milliseconds. Default is 1000.</param>
        public PollingBasedStoreNotifier(Func<CancellationToken, Task<long>> readHeadPosition, int interval = 1000)
        {
            _readHeadPosition = readHeadPosition;
            _interval = interval;
            //Task.Run(Poll, _disposed.Token);
        }

        public Task IsInitialized => _initialized.Task;

        public void Dispose()
        {
            _disposed.Cancel();
        }

        private async Task Poll()
        {
            long headPosition = -2;
            long previousHeadPosition = headPosition;
            while (!_disposed.IsCancellationRequested)
            {
                try
                {
                    headPosition = await _readHeadPosition(_disposed.Token);
                    _initialized.TrySetResult(Unit.Default);
                    if(s_logger.IsTraceEnabled())
                    {
                        s_logger.TraceFormat("Polling head position {headPosition}. Previous {previousHeadPosition}",
                            headPosition, previousHeadPosition);
                    }

                    if (headPosition > previousHeadPosition)
                    {
                        _readonlyStreamStore.NotifyStreamsUpdated(new StreamsUpdated(new Dictionary<string, int>()));
                        previousHeadPosition = headPosition;
                    }
                    else
                    {
                        await Task.Delay(_interval, _disposed.Token);
                    }
                }
                catch(Exception ex)
                {
                    s_logger.ErrorException("Exception occurred polling stream store for messages. " +
                                            $"HeadPosition: {headPosition}", ex);
                    await Task.Delay(_interval, _disposed.Token);
                }
            }
        }
    }
}