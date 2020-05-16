using System;

namespace Kraken.Ado.Command.Mssql
{
    public class MssqlAdoCommanderProvider : IAdoCommanderProvider
    {
        public string Provider => "System.Data.SqlClient";

        public AdoCommander Create(int? commandTimeout = null)
        {
            return new MssqlAdoCommander(commandTimeout);
        }
        public AdoCommander Create(string connectionString, int? commandTimeout = null)
        {
            return new MssqlAdoCommander(connectionString, commandTimeout);
        }

        public AdoCommander Create(string connectionString, string databaseName, int? commandTimeout = null)
        {
            return new MssqlAdoCommander(connectionString, databaseName, commandTimeout);
        }
    }
}
