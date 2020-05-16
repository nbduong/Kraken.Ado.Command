using System;
using System.Data.Common;

namespace Kraken.Ado.Command
{
    public abstract class AdoCommanderGeneric<TDbConnection, TDbCommand, TDbParameter, TDbDataAdapter, TDbCommandBuilder> : AdoCommander
        where TDbConnection : DbConnection
        where TDbCommand : DbCommand
        where TDbParameter : DbParameter
        where TDbDataAdapter : DbDataAdapter
        where TDbCommandBuilder : DbCommandBuilder
    {
        protected AdoCommanderGeneric() : base(null, null, null)
        {

        }
        protected AdoCommanderGeneric(int? commandTimeout) : base(commandTimeout)
        {
        }

        protected AdoCommanderGeneric(string connectionString, int? commandTimeout) : base(connectionString, commandTimeout)
        {
        }

        protected AdoCommanderGeneric(string connectionString, string databaseName, int? commandTimeout) : base(connectionString, databaseName, commandTimeout)
        {
        }

        public override DbConnection CreateDbConnection()
        {
            return CreateConnection();
        }

        public override DbCommand CreateDbCommand()
        {
            return Activator.CreateInstance<TDbCommand>();
        }

        public override DbParameter CreateDbParameter()
        {
            return Activator.CreateInstance<TDbParameter>();
        }

        public override DbDataAdapter CreateDbDataAdapter()
        {
            return Activator.CreateInstance<TDbDataAdapter>();
        }

        public override DbCommandBuilder CreateDbCommandBuilder()
        {
            return Activator.CreateInstance<TDbCommandBuilder>();
        }
    }
}