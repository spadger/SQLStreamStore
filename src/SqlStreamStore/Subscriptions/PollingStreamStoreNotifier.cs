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
    public sealed class PollingStreamStoreNotifier : IStreamStoreNotifier
    {
#if NET461
        private static readonly ILog s_logger = LogProvider.GetCurrentClassLogger();
#elif NETSTANDARD1_3
        private static readonly ILog s_logger = LogProvider.GetLogger("SqlStreamStore.Subscriptions.PollingStreamStoreNotifier");
#endif
        private readonly IReadonlyStreamStore _store;
        private readonly CancellationTokenSource _disposed = new CancellationTokenSource();
        private readonly Func<CancellationToken, Task<long>> _readHeadPosition;
        private readonly int _interval;
        private readonly Subject<IStreamsUpdated> _storeAppended = new Subject<IStreamsUpdated>();

        /// <summary>
        ///     Initializes a new instance of of <see cref="PollingStreamStoreNotifier"/>.
        /// </summary>
        /// <param name="store">The store to poll.</param>
        /// <param name="interval">The interval to poll in milliseconds. Default is 1000.</param>
        public PollingStreamStoreNotifier(IReadonlyStreamStore store, int interval = 1000)
            : this(store.ReadHeadPosition, interval)
        {
            _store = store;
        }

        /// <summary>
        ///     Initializes a new instance of of <see cref="PollingStreamStoreNotifier"/>.
        /// </summary>
        /// <param name="readHeadPosition">An operation to read the head position of a store.</param>
        /// <param name="interval">The interval to poll in milliseconds. Default is 1000.</param>
        public PollingStreamStoreNotifier(Func<CancellationToken, Task<long>> readHeadPosition, int interval = 1000)
        {
            _readHeadPosition = readHeadPosition;
            _interval = interval;
            Task.Run(Poll, _disposed.Token);
        }

        public void Dispose()
        {
            _disposed.Cancel();
        }

        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<IStreamsUpdated> observer) => _storeAppended.Subscribe(observer);

        private async Task Poll()
        {
            long previousHeadPosition = -1;
            while (!_disposed.IsCancellationRequested)
            {
                var headPosition = await ReliablyReadHeadPosition();
/*
                try
                {
                    headPosition = await _readHeadPosition(_disposed.Token);
                    if(s_logger.IsTraceEnabled())
                    {
                        s_logger.TraceFormat("Polling head position {headPosition}. Previous {previousHeadPosition}",
                            headPosition, previousHeadPosition);
                    }
                }
                catch(Exception ex)
                {
                    s_logger.ErrorException($"Exception occurred polling stream store for messages. " +
                                            $"HeadPosition: {headPosition}", ex);
                }*/

                if(headPosition > previousHeadPosition && previousHeadPosition > -1)
                {
                    //await _store.ReadAllForwards(previousHeadPosition)
                    _storeAppended.OnNext(new StreamsUpdated(new Dictionary<string, int>()));
                    previousHeadPosition = headPosition;
                }
                else
                {
                    await Task.Delay(_interval, _disposed.Token);
                }
            }
        }

        private async Task<long> ReliablyReadHeadPosition()
        {
            while (true)
            {
                _disposed.Token.ThrowIfCancellationRequested();
                long headPosition = -1;
                try
                {
                    headPosition = await _readHeadPosition(_disposed.Token);
                    if (s_logger.IsTraceEnabled())
                    {
                        s_logger.TraceFormat("Polling head position {headPosition}.", headPosition);
                    }
                    return headPosition;
                }
                catch (Exception ex)
                {
                    s_logger.ErrorException("Exception occurred polling stream store for messages. " +
                                            $"HeadPosition: {headPosition}",
                        ex);
                }
                await Task.Delay(_interval, _disposed.Token);
            }
        }
    }
}