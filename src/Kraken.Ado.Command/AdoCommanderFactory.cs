using System;

namespace Kraken.Ado.Command
{
    public class AdoCommanderFactory : IAdoCommanderFactory
    {
        private readonly IAdoCommanderProvider _provider;
        private readonly string _connectionString;
        private readonly int? _commandTimeout;
        private readonly string _databaseName;

        public AdoCommanderFactory(IAdoCommanderProvider provider) => _provider = provider ?? throw new ArgumentNullException(nameof(provider));

        public AdoCommanderFactory(string connectionString, IAdoCommanderProvider provider, int? commandTimeout = null)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _commandTimeout = commandTimeout;
        }

        public AdoCommanderFactory(string connectionString, string databaseName, IAdoCommanderProvider provider, int? commandTimeout = null)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _commandTimeout = commandTimeout;
        }

        public AdoCommander Create() => _provider.Create(_connectionString, _databaseName, _commandTimeout);
    }
}