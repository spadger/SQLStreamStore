namespace SqlStreamStore
{
    using SqlStreamStore.Imports.Ensure.That;

    public class PostgresStreamStoreSettings : StreamStoreSettings
    {
        private string _schema = "public";

        public PostgresStreamStoreSettings(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            ConnectionString = connectionString;
        }

        public string ConnectionString { get; private set; }

        public string Schema
        {
            get => _schema;
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    Ensure.That(value, nameof(Schema)).IsNotNullOrWhiteSpace();
                }
                _schema = value;
            }
        }

        public JsonBSupport JsonB { get; set; } = JsonBSupport.AutoDetect;
    }

    public enum JsonBSupport
    {
        AutoDetect,
        Disable
    }
}