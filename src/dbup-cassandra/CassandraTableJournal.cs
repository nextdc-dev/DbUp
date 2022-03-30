using System;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;

namespace dbup_cassandra
{
    public class CassandraTableJournal : TableJournal
    {
        /// <inheritdoc/>
        public CassandraTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, string schema, string table) 
            : base(connectionManager, logger, new CassandraObjectParser(), schema?.ToLower(), table)
        {
        }

        protected override string GetInsertJournalEntrySql(string @scriptName, string @applied)
        {
            return $"insert into {FqSchemaTableName} (id, script_name, applied) values ({Guid.NewGuid()},:" + scriptName.Replace("@", "") + ",:" + applied.Replace("@", "") + ")";
        }

        protected override string GetJournalEntriesSql()
        {
            return $"select script_name from {FqSchemaTableName}";
        }

        protected override string CreateSchemaTableSql(string quotedPrimaryKeyName)
        {
            return
$@"create table {FqSchemaTableName} (
    id UUID PRIMARY KEY,
    script_name text,
    applied timestamp
)";
        }

        protected override string DoesTableExistSql()
        {
            return string.Format("select count(*) from system_schema.tables where keyspace_name='{1}' and table_name = '{0}'", UnquotedSchemaTableName, SchemaTableSchema);
        }
    }
}
