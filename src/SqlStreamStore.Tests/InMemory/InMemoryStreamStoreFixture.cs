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
            IStreamStore store = new InMemoryStreamStore(settings);
            await store.EnableSubscriptions();
            return store;
        }
    }
}