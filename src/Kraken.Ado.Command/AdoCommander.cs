using System;
using System.Data.Common;

namespace Kraken.Ado.Command
{
    public abstract partial class AdoCommander
    {
        public int? CommandTimeout { get; set; }

        public string ConnectionString { get; set; }

        public string DatabaseName { get; set; }

        protected AdoCommander(int? commandTimeout) : this(null, null, commandTimeout)
        {
        }

        protected AdoCommander(string connectionString, int? commandTimeout) : this(connectionString, null, commandTimeout)
        {
        }

        protected AdoCommander(string connectionString, string databaseName, int? commandTimeout)
        {
            ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            CommandTimeout = commandTimeout;
        }

        public abstract DbConnection CreateConnection();

        public abstract DbCommand CreateDbCommand();

        public DbCommand CreateDbCommand(int? commandTimeout)
        {
            var cmd = this.CreateDbCommand();
            if ((commandTimeout ?? 0) > 0)
                cmd.CommandTimeout = commandTimeout.Value;
            else if ((this.CommandTimeout ?? 0) > 0)
                cmd.CommandTimeout = this.CommandTimeout.Value;
            return cmd;
        }

        public abstract DbCommandBuilder CreateDbCommandBuilder();

        public abstract DbConnection CreateDbConnection();

        public abstract DbDataAdapter CreateDbDataAdapter();

        public abstract DbParameter CreateDbParameter();

        protected DbParameter CreateDbParameter(string name, object value)
        {
            var p = this.CreateDbParameter();
            p.ParameterName = name;
            p.Value = value ?? DBNull.Value;
            if (p.Value != DBNull.Value)
                p.DbType = AdoCommandHelper.ConvertToDbType(p.Value.GetType());
            return p;
        }

        protected DbParameter CreateDbParameter(string name, Type type)
        {
            var p = this.CreateDbParameter();
            p.ParameterName = name;
            if (type != null)
                p.DbType = AdoCommandHelper.ConvertToDbType(type);
            return p;
        }
    }
}