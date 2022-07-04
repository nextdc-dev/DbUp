using DbUp.Builder;
using DbUp.Engine.Transactions;

namespace dbup_cassandra
{
    public static class CassandraExtensions
    {
        /// <summary>
        /// Creates an upgrader for Cassandra databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">Cassandra database connection string.</param>
        /// <param name="keyspace">Cassandra keyspace where scripts are executed against and journal logs are written to.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Cassandra databases.
        /// </returns>
        /// <remarks>
        /// Does not support setting up keyspace. Expectation is keyspace is configured appropriately prior to executing.
        /// A script can be executed by DbUp to setup keyspace if no journal is required.
        /// </remarks>
        public static UpgradeEngineBuilder CassandraDatabase(this SupportedDatabases supported, string connectionString, string keyspace = null)
        {
            return CassandraDatabase(new CassandraConnectionManager(connectionString), keyspace);
        }
        /// <summary>
        /// Creates an upgrader for Cassandra databases.
        /// </summary>
        /// <param name="connectionManager">The <see cref="CassandraConnectionManager"/> to be used during a database upgrade.</param>
        /// <param name="keyspace">Cassandra keyspace where scripts are executed against and journal logs are written to.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Cassandra databases.
        /// </returns>
        /// <remarks>
        /// Does not support setting up keyspace. Expectation is keyspace is configured appropriately prior to executing.
        /// A script can be executed by DbUp to setup keyspace if no journal is required.
        /// </remarks>
        public static UpgradeEngineBuilder CassandraDatabase(IConnectionManager connectionManager, string keyspace)
        {
            var builder = new UpgradeEngineBuilder();
            builder.Configure(c => c.ConnectionManager = connectionManager);
            builder.Configure(c => c.ScriptExecutor = new CassandraScriptExecutor(() => c.ConnectionManager,  () => c.Log, keyspace, () => c.VariablesEnabled, c.ScriptPreprocessors, () => c.Journal));
            builder.Configure(c => c.Journal = new CassandraTableJournal(() => c.ConnectionManager, () => c.Log, keyspace, "schemaversions"));
            return builder;
        }


        /// <summary>
        /// Tracks the list of executed scripts in a table.
        /// </summary>
        /// <param name="builder">The builder.</param>
        /// <param name="keyspace">The Cassandra keyspace to log upgrades to.</param>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        public static UpgradeEngineBuilder JournalToTable(this UpgradeEngineBuilder builder, string keyspace, string table)
        {
            builder.Configure(c => c.Journal = new CassandraTableJournal(() => c.ConnectionManager, () => c.Log, keyspace, table));
            return builder;
        }
    }
}
