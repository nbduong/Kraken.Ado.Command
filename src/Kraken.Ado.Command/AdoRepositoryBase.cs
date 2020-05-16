using System;

namespace Kraken.Ado.Command
{
    public class AdoRepositoryBase<TEntity> : IAdoRepository<TEntity> where TEntity : class
    {
        public IAdoCommanderFactory _commanderFactory { get; }

        public AdoRepositoryBase(IAdoCommanderFactory commanderFactory)
        {
            _commanderFactory = commanderFactory ?? throw new ArgumentNullException(nameof(commanderFactory));
        }
    }
}