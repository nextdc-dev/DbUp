using System;
using System.Collections.Generic;
using Cassandra;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;

namespace dbup_cassandra
{
    public class CassandraScriptExecutor : ScriptExecutor
    {
        public CassandraScriptExecutor(Func<IConnectionManager> connectionManagerFactory, Func<IUpgradeLog> log, string keyspace, Func<bool> variablesEnabled,
            IEnumerable<IScriptPreprocessor> scriptPreprocessors, Func<IJournal> journalFactory) 
            : base(connectionManagerFactory, new CassandraObjectParser(), log, keyspace, variablesEnabled, scriptPreprocessors, journalFactory)
        {
        }

        protected override void ExecuteCommandsWithinExceptionHandler(int index, SqlScript script, Action executeCommand)
        {
            try
            {
                executeCommand();
            }
            catch (DriverException cqlException)
            {
                Log().WriteInformation("CQL Driver exception has occured in script: '{0}'", script.Name);
                Log().WriteError("Script block number: {0}; Message: {1}", index, cqlException.Message);
                Log().WriteError(cqlException.ToString());
                throw;
            }
        }

        protected override string GetVerifySchemaSql(string keyspace)
        {
            return $"USE {keyspace};";
        }
    }
}
