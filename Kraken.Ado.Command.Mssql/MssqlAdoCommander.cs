using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace Kraken.Ado.Command.Mssql
{
    public class MssqlAdoCommander : AdoCommanderGeneric<SqlConnection, SqlCommand, SqlParameter, SqlDataAdapter, SqlCommandBuilder>
    {
        private readonly string _connectionString;
        private readonly int? _commandTimeout;
        private readonly string _databaseName;

        public MssqlAdoCommander(int? commandTimeout) => this._commandTimeout = commandTimeout;

        public MssqlAdoCommander(string connectionString, int? commandTimeout)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _commandTimeout = commandTimeout;
        }

        public MssqlAdoCommander(string connectionString, string databaseName, int? commandTimeout)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _commandTimeout = commandTimeout;
        }

        public override DbConnection CreateConnection()
        {
            DbConnection conn = null;
            if (string.IsNullOrWhiteSpace(this.ConnectionString))
                conn = new SqlConnection();
            else
                conn = new SqlConnection(this.ConnectionString);

            if (!string.IsNullOrWhiteSpace(this.DatabaseName))
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.ChangeDatabase(this.DatabaseName);
                }
                else
                {
                    conn.StateChange += DatabaseConnection_StateChanged;
                }
            }

            return conn;
        }

        private void DatabaseConnection_StateChanged(object sender, StateChangeEventArgs e)
        {
            var con = (DbConnection)sender;
            if (con.State == ConnectionState.Open)
            {
                if (!string.IsNullOrWhiteSpace(DatabaseName))
                    con.ChangeDatabase(this.DatabaseName);
            }
        }

        public override List<string> GetTables() => new List<string>();
    }
}