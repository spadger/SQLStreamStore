namespace SqlStreamStore.InMemory
{
    using System.Threading.Tasks;
    using SqlStreamStore;

    public class InMemoryStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public override async Task<IStreamStore> GetStreamStore()
        {
            var settings = new StreamStoreSettings
            {
                GetUtcNow = () => GetUtcNow()
            };
            IStreamStore streamStore = new InMemoryStreamStore(settings);
            await streamStore.Notifier.IsInitialized;
            return streamStore;
        }
    }
}