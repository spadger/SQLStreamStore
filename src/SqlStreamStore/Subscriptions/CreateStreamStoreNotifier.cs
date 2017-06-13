﻿namespace SqlStreamStore.Subscriptions
{
    using SqlStreamStore;

    /// <summary>
    ///     Represents an operaion to create a stream store notifier.
    /// </summary>
    /// <param name="readonlyStreamStore"></param>
    /// <returns></returns>
    public delegate IStreamStoreNotifier CreateStoreUpdatedNotifier(IReadonlyStreamStore readonlyStreamStore);
}