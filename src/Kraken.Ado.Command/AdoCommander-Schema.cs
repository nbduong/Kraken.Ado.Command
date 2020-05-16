using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Kraken.Ado.Command
{
    public partial class AdoCommander : IDisposable
    {
        private DataSet _schema = new DataSet();
        private readonly object _schemaLock = new object();

        public void ClearSchemaCache()
        {
            lock (_schemaLock)
            {
                _schema = new DataSet();
            }
        }

        public void Dispose()
        {
            _schema.Dispose();
            GC.SuppressFinalize(this);
        }

        public DataSet GetDatabaseSchema()
        {
            var schemaDataSet = new DataSet();
            foreach (var tableName in GetTables())
            {
                var schemaTable = GetTableSchema(tableName);
                schemaTable = schemaTable.Copy();
                schemaDataSet.Tables.Add(schemaTable);
            }

            schemaDataSet.AcceptChanges();

            return schemaDataSet;
        }

        public string GetDefaultSortField(string table)
        {
            var tableSchema = GetTableSchema(table);
            if (tableSchema.PrimaryKey.Length == 1)
                return tableSchema.PrimaryKey[0].ColumnName;

            var column = tableSchema.Columns.Cast<DataColumn>().FirstOrDefault(x => x.ColumnName.EndsWith("Name", StringComparison.OrdinalIgnoreCase));
            if (column == null)
            {
                column = tableSchema.Columns.Cast<DataColumn>().FirstOrDefault(x => x.ColumnName.EndsWith("ID", StringComparison.OrdinalIgnoreCase));
                if (column == null)
                {
                    column = tableSchema.Columns.Cast<DataColumn>().FirstOrDefault(x => x.DataType == typeof(int) || x.DataType == typeof(long));
                    if (column == null)
                    {
                        column = tableSchema.Columns.Cast<DataColumn>().FirstOrDefault();
                        if (column == null)
                            throw new ArgumentOutOfRangeException("table", "Table [" + table + "] has not a sortable field.");
                    }
                }
            }
            return column.ColumnName;
        }

        public virtual T GetMax<T>(string table, string field)
        {
            var max = ExecuteScalar($"SELECT max({QuoteIdentifier(field)}) FROM {QuoteIdentifier(table)}");
            return (max == null || max == DBNull.Value) ? default(T) : (T)max;
        }

        public virtual int GetMaxInt32(string table, string field)
        {
            var max = ExecuteScalar($"SELECT max({QuoteIdentifier(field)}) FROM {QuoteIdentifier(table)}");
            return (max == null || max == DBNull.Value) ? 0 : Convert.ToInt32(max);
        }

        public virtual long GetMaxInt64(string table, string field)
        {
            var max = ExecuteScalar($"SELECT max({QuoteIdentifier(field)}) FROM {QuoteIdentifier(table)}");
            return (max == null || max == DBNull.Value) ? 0 : Convert.ToInt64(max);
        }

        public string GetSinglePrimaryField(string table)
        {
            var tableSchema = GetTableSchema(table);
            if (tableSchema.PrimaryKey.Length == 1)
                return tableSchema.PrimaryKey[0].ColumnName;
            throw new ArgumentOutOfRangeException(nameof(table), "Table [" + table + "] has not a single primary field.");
        }

        public abstract List<string> GetTables();

        public DataTable GetTableSchema(string table)
        {
            lock (_schemaLock)
            {
                if (!_schema.Tables.Contains(table))
                {
                    var ds = GetSchema("SELECT * FROM " + QuoteIdentifier(table) + ";");
                    ds.Tables[0].TableName = table;
                    _schema.Merge(ds);
                }
                return _schema.Tables[table];
            }
        }

        public string QuoteIdentifier(string unquotedIdentifier) => CommandBuilder.QuoteIdentifier(unquotedIdentifier);
    }
}