using DbUp.Support;

namespace dbup_cassandra
{
    public class CassandraObjectParser : SqlObjectParser
    {
        public CassandraObjectParser() : base(string.Empty, string.Empty)
        {
        }

        public override string UnquoteIdentifier(string objectName)
        {
            return objectName;
        }
    }
}
