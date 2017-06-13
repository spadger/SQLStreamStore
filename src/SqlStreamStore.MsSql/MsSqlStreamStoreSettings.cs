namespace SqlStreamStore
{
    using SqlStreamStore.Imports.Ensure.That;

    /// <summary>
    ///     Represents setting to configure a <see cref="MsSqlStreamStore"/>
    /// </summary>
    public class MsSqlStreamStoreSettings : StreamStoreSettings
    {
        private string _schema = "dbo";

        /// <summary>
        ///     Initialized a new instance of <see cref="MsSqlStreamStoreSettings"/>.
        /// </summary>
        /// <param name="connectionString">A connection string to the </param>
        public MsSqlStreamStoreSettings(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            ConnectionString = connectionString;
            LogName = "MsSqlStreamStore";
        }

        /// <summary>
        ///     Gets the connection string.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        ///     MsSqlStream store supports stores in a single database through 
        ///     the useage of schema. This is useful if you want to contain
        ///     multiple bounded contexts in a single database. Alternative is
        ///     use a database per bounded context, which may be more appropriate
        ///     for larger stores.
        /// </summary>
        public string Schema
        {
            get => _schema;
            set
            {
                Ensure.That(value, nameof(Schema)).IsNotNullOrWhiteSpace();
                _schema = value;
            }
        }
    }
}