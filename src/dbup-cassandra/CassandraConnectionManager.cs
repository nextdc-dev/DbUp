using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Cassandra.Data;
using DbUp.Engine.Transactions;

namespace dbup_cassandra
{
    public class CassandraConnectionManager : DatabaseConnectionManager
    {
        /// <summary>
        /// Creates new Cassandra Connection Manager
        /// </summary>
        public CassandraConnectionManager(string connectionString) : base(_ => new CqlConnection(connectionString))
        {
        
        }

        /// <summary>
        /// Cassandra statements separator is ; (https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/cqlSyntax.html)
        /// </summary>
        public override IEnumerable<string> SplitScriptIntoCommands(string scriptContents)
        {
            var scriptStatements =
                Regex.Split(scriptContents, "^\\s*;\\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline)
                    .Select(x => x.Trim())
                    .Where(x => x.Length > 0)
                    .ToArray();

            return scriptStatements;
        }
    }
}
