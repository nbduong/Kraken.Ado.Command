namespace Kraken.Ado.Command
{
    public interface IAdoCommanderProvider
    {
        string Provider { get; }

        AdoCommander Create(int? commandTimeout = null);

        AdoCommander Create(string connectionString, int? commandTimeout = null);

        AdoCommander Create(string connectionString, string databaseName, int? commandTimeout = null);
    }
}