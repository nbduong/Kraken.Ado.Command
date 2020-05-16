namespace Kraken.Ado.Command
{
    public interface IAdoRepository<TEntity> where TEntity : class
    {
        IAdoCommanderFactory _commanderFactory { get; }
    }
}