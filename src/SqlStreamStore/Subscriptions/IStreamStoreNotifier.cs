namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;

    /// <summary>
    ///     Represents an notifier lets subsribers know that the 
    ///     stream store has new messages.
    /// </summary>
    public interface IStreamStoreNotifier : IObservable<IStreamsUpdated>, IDisposable
    {}

    public interface IStreamsUpdated : IReadOnlyDictionary<string, int>
    {}

    public sealed class StreamsUpdated : ReadOnlyDictionary<string, int>, IStreamsUpdated
    {
        public StreamsUpdated(IDictionary<string, int> dictionary) : base(dictionary)
        {}
    }
}