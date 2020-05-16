using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Transactions;

namespace Kraken.Ado.Command
{
    public partial class AdoCommander
    {
        internal event Action<IEnumerable<DataChange>> DataChanged;

        internal void FireTableChanged(IEnumerable<DataChange> dataChanges)
        {
            if (DataChanged != null && dataChanges != null)
                DataChanged?.Invoke(dataChanges);
        }

        private object _transactionDataChangesLock = new object();
        private List<DataChange> _transactionDataChanges;

        private void RequestFireDataChanged(DataChange dataChange)
        {
            if (DataChanged == null)
                return;

            var trans = Transaction.Current;
            if (trans != null)
            {
                FireTableChanged(new DataChange[] { dataChange });
            }
            else
            {
                lock (_transactionDataChangesLock)
                {
                    if (_transactionDataChanges == null)
                        _transactionDataChanges = new List<DataChange>();
                    _transactionDataChanges.Add(dataChange);

                    if (trans.TransactionInformation.Status == TransactionStatus.Committed)
                    {
                        FireTableChanged(_transactionDataChanges);
                        _transactionDataChanges.Clear();
                    }
                    else if (trans.TransactionInformation.Status != TransactionStatus.Active)
                    {
                        _transactionDataChanges.Clear();
                    }
                }
            }
        }

        private void RequestFireDataChangedByStatement(string sqlStatement, int changeCount)
        {
            if (DataChanged == null || changeCount <= 0)
                return;
            if (AdoCommandHelper.IsSqlDDLStatement(sqlStatement, out var command, out var table))
            {
                var dataChange = new DataChange
                {
                    ChangeInfo = table,
                    ChangedTableGetter = args => new string[] { (string)args },
                };
                if (command == 1)
                    dataChange.IsInsertOnly = true;
                else if (command == 2)
                    dataChange.IsUpdateOnly = true;
                else if (command == 3)
                    dataChange.IsDeleteOnly = true;

                RequestFireDataChanged(dataChange);
            }
        }

        private DataChange BuildInsertDataChange(string table, string[] fieldNames, IEnumerable<object[]> insertValueSets) => new DataChange
        {
            ChangeInfo = table,
            ChangedTableGetter = args => new string[] { (string)args },
            ChangeGetter = args =>
            {
                var ds = new DataSet();
                var dt = GetTableSchema(table);
                ds.Tables.Add(dt);

                dt.Columns.Cast<DataColumn>().ToList().ForEach(col =>
                {
                    if (!fieldNames.Contains(col.ColumnName))
                        dt.Columns.Remove(col);
                });

                foreach (var insertValues in insertValueSets)
                {
                    var dr = dt.NewRow();
                    for (var i = 0; i < fieldNames.Length; i++)
                        dr[fieldNames[i]] = insertValues[i];
                    dt.Rows.Add(dr);
                }

                return ds;
            },
        };

        private DataChange BuildUpdateDataChange(string table, string[] conditionFields, object[] conditionValues) => new DataChange
        {
            ChangeInfo = table,
            ChangedTableGetter = args => new string[] { (string)args },
            ChangeGetter = args =>
            {
                var ds = ExecuteTableQueryToDataSet(table, conditionFields, conditionValues);
                ds.Tables[0].Rows.Cast<DataRow>().ToList().ForEach(x => x.SetModified());
                return ds;
            },
            ConditionGetter = args =>
            {
                var conditions = new Dictionary<string, object>();
                for (var i = 0; i < conditionFields.Length; i++)
                    conditions[conditionFields[i]] = conditionValues[i];

                return conditions;
            },
        };

        private DataChange BuildDeleteDataChange(string table, string conditionField, object conditionValue) => BuildDeleteDataChange(table, new string[] { conditionField }, new object[] { conditionValue });

        private static DataChange BuildDeleteDataChange(string table, string[] conditionFields, object[] conditionValues) => new DataChange
        {
            ChangeInfo = table,
            ChangedTableGetter = args => new string[] { (string)args },
            ConditionGetter = args =>
            {
                var conditions = new Dictionary<string, object>();
                for (var i = 0; i < conditionFields.Length; i++)
                    conditions[conditionFields[i]] = conditionValues[i];

                return conditions;
            },
        };
    }
}