using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Kraken.Ado.Command
{
    public partial class AdoCommander
    {
        private DbCommandBuilder _commandBuilder;

        private DbCommandBuilder CommandBuilder => _commandBuilder ?? (_commandBuilder = CreateDbCommandBuilder());

        public DataSet GetSchema(string sqlQuery, int? commandTimeout = null)
        {
            using (var con = CreateConnection())
            {
                con.Open();
                using (var cmd = CreateDbCommand(commandTimeout))
                {
                    cmd.CommandText = sqlQuery;
                    cmd.Connection = con;

                    using (var adapter = CreateDbDataAdapter())
                    {
                        adapter.SelectCommand = cmd;
                        adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                        var ds = new DataSet();
                        adapter.FillSchema(ds, SchemaType.Source);
                        return ds;
                    }
                }
            }
        }

        public DataSet ExecuteQueryToDataSet(string sqlQuery, int? commandTimeout = null)
        {
            using (var con = CreateConnection())
            {
                con.Open();
                using (var cmd = CreateDbCommand(commandTimeout))
                {
                    cmd.CommandText = sqlQuery;
                    cmd.Connection = con;

                    using (var adapter = CreateDbDataAdapter())
                    {
                        adapter.SelectCommand = cmd;
                        adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;
                        var ds = new DataSet
                        {
                            EnforceConstraints = false
                        };
                        adapter.Fill(ds);
                        return ds;
                    }
                }
            }
        }

        public DataSet ExecuteQueryWithParamToDataSet(string sqlQuery, string[] paramNames, object[] paramValues, int? commandTimeout = null)
        {
            var N = 0;
            if (paramNames != null && paramValues != null)
            {
                N = Math.Min(paramNames.Length, paramValues.Length);
                using (var con = CreateConnection())
                {
                    con.Open();
                    using (var cmd = CreateDbCommand(commandTimeout))
                    {
                        cmd.CommandText = sqlQuery;
                        cmd.Connection = con;

                        for (var i = 0; i < N; i++)
                        {
                            var param = CreateDbParameter(paramNames[i], paramValues[i]);
                            cmd.Parameters.Add(param);
                        }
                        using (var adapter = CreateDbDataAdapter())
                        {
                            adapter.SelectCommand = cmd;
                            adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;

                            var ds = new DataSet
                            {
                                EnforceConstraints = false
                            };
                            adapter.Fill(ds);
                            return ds;
                        }
                    }
                }
            }
            return default(DataSet);
        }

        public DataSet ExecuteQueryWithParamToDataSet(string sqlQuery, string[] paramNames, DbType[] paramTypes, object[] paramValues, int? commandTimeout = null)
        {
            var N = 0;
            if (paramNames != null && paramValues != null && paramTypes != null)
            {
                N = Math.Min(paramNames.Length, paramValues.Length);
                N = Math.Min(N, paramTypes.Length);
                using (var con = CreateConnection())
                {
                    con.Open();
                    using (var cmd = CreateDbCommand(commandTimeout))
                    {
                        cmd.CommandText = sqlQuery;
                        cmd.Connection = con;

                        for (var i = 0; i < N; i++)
                        {
                            var param = CreateDbParameter();
                            param.ParameterName = paramNames[i];
                            param.DbType = paramTypes[i];
                            param.Value = paramValues[i] ?? DBNull.Value;
                            cmd.Parameters.Add(param);
                        }
                        using (var adapter = CreateDbDataAdapter())
                        {
                            adapter.SelectCommand = cmd;
                            adapter.MissingSchemaAction = MissingSchemaAction.AddWithKey;

                            var ds = new DataSet
                            {
                                EnforceConstraints = false
                            };
                            adapter.Fill(ds);
                            return ds;
                        }
                    }
                }
            }

            return default(DataSet);
        }

        protected string BuildSelectFields(string tableName, params string[] fieldNames)
        {
            if (fieldNames == null || fieldNames.Length == 0)
            {
                var table = GetTableSchema(tableName);
                fieldNames = table.Columns.OfType<DataColumn>().Select(x => x.ColumnName).ToArray();
            }
            return string.Join(", ", fieldNames.Select(x => QuoteIdentifier(x)));
        }

        protected string BuildSelectFields(params string[] fieldNames)
        {
            if (fieldNames == null || fieldNames.Length == 0)
                return "*";
            return string.Join(", ", fieldNames.Select(x => QuoteIdentifier(x)));
        }

        public IEnumerable<string> GetTableFields(string tableName, string[] excludedFields = null)
        {
            var table = GetTableSchema(tableName);
            var fieldNames = table.Columns.OfType<DataColumn>().Select(x => x.ColumnName).AsEnumerable();
            if (excludedFields != null && excludedFields.Any())
                fieldNames = fieldNames.Where(f => !excludedFields.Contains(f));
            return fieldNames;
        }

        public DataSet ExecuteTableQueryToDataSet(string table, string[] fieldNames = null, int? commandTimeout = null) => ExecuteQueryToDataSet(string.Format("SELECT {0} FROM {1};", BuildSelectFields(fieldNames), QuoteIdentifier(table)), commandTimeout: commandTimeout);

        public DataSet ExecuteTableQueryToDataSet(string table, string[] fieldNames, string condition, int? commandTimeout = null) => ExecuteQueryToDataSet(string.Format("SELECT {0} FROM {1} WHERE {2};", BuildSelectFields(fieldNames), QuoteIdentifier(table), condition), commandTimeout: commandTimeout);

        public DataSet ExecuteTableQueryToDataSet(string table, string conditionField, object conditionValue, int? commandTimeout = null) => ExecuteTableQueryToDataSet(table, null, conditionField, conditionValue, commandTimeout: commandTimeout);

        public DataSet ExecuteTableQueryToDataSet(string table, string[] fieldNames, string conditionField, object conditionValue, int? commandTimeout = null) => ExecuteQueryWithParamToDataSet(string.Format("SELECT {0} FROM {1} WHERE {2} = @conditionValue;",
                BuildSelectFields(fieldNames),
                QuoteIdentifier(table),
                QuoteIdentifier(conditionField)),
                new string[] { "@conditionValue" }, new object[] { conditionValue },
                commandTimeout: commandTimeout);

        public DataSet ExecuteTableQueryToDataSetIn(string table, string conditionField, object[] conditionValues, int? commandTimeout = null) => ExecuteTableQueryToDataSetIn(table, null, conditionField, conditionValues, commandTimeout: commandTimeout);

        public DataSet ExecuteTableQueryToDataSetIn(string table, string[] fieldNames, string conditionField, object[] conditionValues, int? commandTimeout = null)
        {
            var paramNames = new string[conditionValues.Length];
            for (var i = 0; i < paramNames.Length; i++)
                paramNames[i] = "@" + conditionField + "_" + i;
            return ExecuteQueryWithParamToDataSet(string.Format("SELECT {0} FROM {1} WHERE {2} IN ({3});",
                BuildSelectFields(fieldNames),
                QuoteIdentifier(table), QuoteIdentifier(conditionField), string.Join(",", paramNames)),
                paramNames,
                conditionValues,
                commandTimeout: commandTimeout);
        }

        public DataSet ExecuteTableQueryToDataSet(string table, string[] conditionFields, object[] conditionValues, int? commandTimeout = null) => ExecuteTableQueryToDataSet(table, null, conditionFields, conditionValues, commandTimeout: commandTimeout);

        public DataSet ExecuteTableQueryToDataSet(string table, string[] fieldNames, string[] conditionFields, object[] conditionValues, int? commandTimeout = null)
        {
            var index = 0;
            var paramNames = AdoCommandHelper.CreateArray("@condition{0}", conditionFields.Length);

            return ExecuteQueryWithParamToDataSet(string.Format("SELECT {0} FROM {1} WHERE {2};",
                BuildSelectFields(fieldNames),
                QuoteIdentifier(table)
                , string.Join(" AND ", conditionFields.Select(x => string.Format("{0} = @condition{1}", QuoteIdentifier(x), index++)))),
                paramNames, conditionValues,
                commandTimeout: commandTimeout);
        }

        public DataSet ExecuteTableQueryToDataSet(string table, IDictionary<string, object> conditions, int? commandTimeout = null) => ExecuteTableQueryToDataSet(table, null, conditions, commandTimeout: commandTimeout);

        public DataSet ExecuteTableQueryToDataSet(string table, string[] fieldNames, IDictionary<string, object> conditions, int? commandTimeout = null)
        {
            var index = 0;
            var conditionFieldNames = new string[conditions.Count];
            var conditionFieldValues = new object[conditions.Count];
            foreach (var condition in conditions)
            {
                conditionFieldNames[index] = condition.Key;
                conditionFieldValues[index] = condition.Value;
                index++;
            }

            return ExecuteTableQueryToDataSet(table, fieldNames, conditionFieldNames, conditionFieldValues, commandTimeout: commandTimeout);
        }

        public object ExecuteScalar(string sqlScalarQuery, int? commandTimeout = null)
        {
            using (var con = CreateConnection())
            {
                con.Open();
                using (var cmd = CreateDbCommand(commandTimeout))
                {
                    cmd.CommandText = sqlScalarQuery;
                    cmd.Connection = con;

                    var result = cmd.ExecuteScalar();
                    return result == DBNull.Value ? null : result;
                }
            }
        }

        public object ExecuteScalarWithParam(string sqlScalarQuery, string[] paramNames, object[] paramValues, int? commandTimeout = null)
        {
            var N = 0;
            if (paramNames != null && paramValues != null)
            {
                N = Math.Min(paramNames.Length, paramValues.Length);
                using (var con = CreateConnection())
                {
                    con.Open();
                    using (var cmd = CreateDbCommand(commandTimeout))
                    {
                        cmd.CommandText = sqlScalarQuery;
                        cmd.Connection = con;

                        for (var i = 0; i < N; i++)
                        {
                            var param = CreateDbParameter(paramNames[i], paramValues[i]);
                            cmd.Parameters.Add(param);
                        }
                        var result = cmd.ExecuteScalar();
                        return result == DBNull.Value ? null : result;
                    }
                }
            }
            return default(object);
        }

        public object ExecuteScalarWithParam(string sqlScalarQuery, string[] paramNames, DbType[] paramTypes, object[] paramValues, int? commandTimeout = null)
        {
            var N = 0;
            if (paramNames != null && paramValues != null && paramTypes != null)
            {
                N = Math.Min(paramNames.Length, paramValues.Length);
                N = Math.Min(N, paramTypes.Length);
                using (var con = CreateConnection())
                {
                    con.Open();
                    using (var cmd = CreateDbCommand(commandTimeout))
                    {
                        cmd.CommandText = sqlScalarQuery;
                        cmd.Connection = con;

                        for (var i = 0; i < N; i++)
                        {
                            var param = CreateDbParameter();
                            param.ParameterName = paramNames[i];
                            param.DbType = paramTypes[i];
                            param.Value = paramValues[i] ?? DBNull.Value;
                            cmd.Parameters.Add(param);
                        }
                        var result = cmd.ExecuteScalar();
                        return result == DBNull.Value ? null : result;
                    }
                }
            }
            else
            {
                return default(object);
            }
        }

        public object ExecuteScalar(string table, string scalarField, string conditionField, object conditionValue, int? commandTimeout = null) => ExecuteScalar(table, scalarField, new string[] { conditionField }, new object[] { conditionValue }, commandTimeout: commandTimeout);

        public object ExecuteScalar(string table, string scalarField, IDictionary<string, object> conditions, int? commandTimeout = null)
        {
            var index = 0;
            var conditionFieldNames = new string[conditions.Count];
            var conditionFieldValues = new object[conditions.Count];
            foreach (var condition in conditions)
            {
                conditionFieldNames[index] = condition.Key;
                conditionFieldValues[index] = condition.Value;
                index++;
            }

            return ExecuteScalar(table, scalarField, conditionFieldNames, conditionFieldValues, commandTimeout: commandTimeout);
        }

        public object ExecuteScalarIn(string table, string scalarField, string conditionField, object[] conditionValues, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(scalarField))
                scalarField = "1";
            else
                scalarField = QuoteIdentifier(scalarField);

            var paramNames = AdoCommandHelper.CreateArray("@condition{0}", conditionValues.Length);

            return ExecuteScalarWithParam(
                string.Format("SELECT {0} FROM {1} WHERE {2} IN ({3});",
                    scalarField,
                    QuoteIdentifier(table),
                    QuoteIdentifier(conditionField),
                    string.Join(",", paramNames)),
                paramNames, conditionValues,
                commandTimeout: commandTimeout);
        }

        public object ExecuteScalar(string table, string scalarField, string[] conditionFields, object[] conditionValues, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(scalarField))
                scalarField = "1";
            else
                scalarField = QuoteIdentifier(scalarField);

            var index = 0;
            var paramNames = AdoCommandHelper.CreateArray("@condition{0}", conditionFields.Length);

            return ExecuteScalarWithParam(string.Format("SELECT {0} FROM {1} WHERE {2};",
                scalarField,
                QuoteIdentifier(table),
                string.Join(" AND ", conditionFields.Select(x => string.Format("{0} = @condition{1}", QuoteIdentifier(x), index++)))),
                paramNames, conditionValues,
                commandTimeout: commandTimeout);
        }

        public long ExecuteTableCount(string table, int? commandTimeout = null)
        {
            var count = ExecuteScalar(string.Format("SELECT COUNT(1) FROM {0};", QuoteIdentifier(table)), commandTimeout: commandTimeout);
            return Convert.ToInt64(count);
        }

        public long ExecuteTableCount(string table, string condition, string[] parameterNames, object[] parameterValues, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(condition))
                return ExecuteTableCount(table);
            var count = ExecuteScalarWithParam(
                string.Format("SELECT COUNT(1) FROM {0} WHERE {1};", QuoteIdentifier(table), condition),
                parameterNames, parameterValues, commandTimeout: commandTimeout);
            return Convert.ToInt64(count);
        }

        public long ExecuteTableCount(string table, IDictionary<string, object> conditions, int? commandTimeout = null)
        {
            var index = 0;
            var conditionFieldNames = new string[conditions.Count];
            var conditionFieldValues = new object[conditions.Count];
            foreach (var condition in conditions)
            {
                conditionFieldNames[index] = condition.Key;
                conditionFieldValues[index] = condition.Value;
                index++;
            }

            return ExecuteTableCount(table: table, conditionFields: conditionFieldNames, conditionValues: conditionFieldValues, commandTimeout: commandTimeout);
        }

        public long ExecuteTableCountIn(string table, string conditionField, object[] conditionValues, int? commandTimeout = null)
        {
            var paramNames = AdoCommandHelper.CreateArray("@condition{0}", conditionValues.Length);

            var count = ExecuteScalarWithParam(
                string.Format("SELECT COUNT(1) FROM {0} WHERE {1} IN ({2});",
                    QuoteIdentifier(table),
                    QuoteIdentifier(conditionField),
                    string.Join(",", paramNames)),
                paramNames, conditionValues,
                commandTimeout: commandTimeout);

            return Convert.ToInt64(count);
        }

        public long ExecuteTableCount(string table, string conditionField, object conditionValue, int? commandTimeout = null)
        {
            var count = ExecuteTableCount(table, new string[] { conditionField }, new object[] { conditionValue }, commandTimeout: commandTimeout);
            return Convert.ToInt64(count);
        }

        public long ExecuteTableCount(string table, string[] conditionFields, object[] conditionValues, int? commandTimeout = null)
        {
            var index = 0;
            var paramNames = AdoCommandHelper.CreateArray("@condition{0}", conditionFields.Length);

            var count = ExecuteScalarWithParam(string.Format("SELECT COUNT(1) FROM {0} WHERE {1};",
                QuoteIdentifier(table),
                string.Join(" AND ", conditionFields.Select(x => string.Format("{0} = @condition{1}", QuoteIdentifier(x), index++)))),
                paramNames, conditionValues,
                commandTimeout: commandTimeout);

            return Convert.ToInt64(count);
        }

        public bool CheckExist(string table, string conditionField, object conditionValue, int? commandTimeout = null)
        {
            var result = ExecuteScalarWithParam(string.Format("SELECT 1 FROM {0} WHERE {1} = @conditionValue;",
                QuoteIdentifier(table),
                QuoteIdentifier(conditionField)),
                new string[] { "@conditionValue" }, new object[] { conditionValue },
                commandTimeout: commandTimeout);

            return result != null && result != DBNull.Value;
        }

        public bool CheckExistIn(string table, string conditionField, object[] conditionValues, int? commandTimeout = null)
        {
            var paramNames = new string[conditionValues.Length];
            for (var i = 0; i < paramNames.Length; i++)
                paramNames[i] = "@" + conditionField + "_" + i;

            var result = ExecuteScalarWithParam(string.Format("SELECT 1 FROM {0} WHERE {1} IN ({2});",
                QuoteIdentifier(table),
                QuoteIdentifier(conditionField),
                string.Join(",", paramNames)),
                paramNames, conditionValues,
                commandTimeout: commandTimeout);

            return result != null && result != DBNull.Value;
        }

        public bool CheckExist(string table, string[] conditionFields, object[] conditionValues, int? commandTimeout = null)
        {
            var index = 0;
            var paramNames = AdoCommandHelper.CreateArray("@condition{0}", conditionFields.Length);

            var result = ExecuteScalarWithParam(string.Format("SELECT 1 FROM {0} WHERE {1};",
                QuoteIdentifier(table)
                , string.Join(" AND ", conditionFields.Select(x => string.Format("{0} = @condition{1}", QuoteIdentifier(x), index++)))),
                paramNames, conditionValues,
                commandTimeout: commandTimeout);

            return result != null && result != DBNull.Value;
        }

        public bool CheckExist(string table, IDictionary<string, object> conditions, int? commandTimeout = null)
        {
            var index = 0;
            var conditionFieldNames = new string[conditions.Count];
            var conditionFieldValues = new object[conditions.Count];
            foreach (var condition in conditions)
            {
                conditionFieldNames[index] = condition.Key;
                conditionFieldValues[index] = condition.Value;
                index++;
            }

            return CheckExist(table, conditionFieldNames, conditionFieldValues, commandTimeout: commandTimeout);
        }

        public void UpdateDataSet(DataSet dataset, int? commandTimeout = null)
        {
            dataset = dataset.GetChanges();
            if (dataset == null)
                return;

            var changeCount = 0;
            using (var con = CreateConnection())
            {
                con.Open();
                using (var trans = con.BeginTransaction())
                {
                    foreach (DataTable table in dataset.Tables)
                    {
                        using (var cmd = CreateDbCommand(commandTimeout))
                        {
                            cmd.CommandText = string.Format("SELECT {0} FROM {1}",
                                string.Join(",", table.Columns.Cast<DataColumn>().Select(x => QuoteIdentifier(x.ColumnName))),
                                QuoteIdentifier(table.TableName));
                            cmd.Connection = con;
                            using (var adapter = CreateDbDataAdapter())
                            {
                                using (var cmdBuilder = CreateDbCommandBuilder())
                                {
                                    adapter.SelectCommand = cmd;
                                    cmdBuilder.DataAdapter = adapter;
                                    changeCount += adapter.Update(table);
                                }
                            }
                        }
                    }
                    trans.Commit();
                }
            }

            if (changeCount > 0)
            {
                var dataChange = new DataChange
                {
                    ChangeInfo = dataset.Copy(),
                    ChangeGetter = arg => (DataSet)arg,
                };

                RequestFireDataChanged(dataChange);
            }
        }

        public int ExecuteNonQuery(string sqlNonQuery, int? commandTimeout = null) => ExecuteNonQuery(sqlNonQuery, true, commandTimeout: commandTimeout);

        private int ExecuteNonQuery(string sqlNonQuery, bool fireDataChange, int? commandTimeout = null)
        {
            var result = 0;
            using (var con = CreateConnection())
            {
                con.Open();
                using (var cmd = CreateDbCommand(commandTimeout))
                {
                    cmd.CommandText = sqlNonQuery;
                    cmd.Connection = con;
                    result = cmd.ExecuteNonQuery();
                }
            }

            if (fireDataChange && result > 0)
                RequestFireDataChangedByStatement(sqlNonQuery, result);
            return result;
        }

        public int ExecuteNonQueryWithParam(string sqlNonQuery, string[] paramNames, object[] paramValues, int? commandTimeout = null) => ExecuteNonQueryWithParam(sqlNonQuery, paramNames, paramValues, true, commandTimeout: commandTimeout);

        private int ExecuteNonQueryWithParam(string sqlNonQuery, string[] paramNames, object[] paramValues, bool fireDataChange, int? commandTimeout = null)
        {
            var N = 0;
            if (paramNames != null && paramValues != null)
            {
                N = Math.Min(paramNames.Length, paramValues.Length);
                var result = 0;
                using (var con = CreateConnection())
                {
                    con.Open();
                    using (var cmd = CreateDbCommand(commandTimeout))
                    {
                        cmd.CommandText = sqlNonQuery;
                        cmd.Connection = con;
                        for (var i = 0; i < N; i++)
                        {
                            var param = CreateDbParameter(paramNames[i], paramValues[i]);
                            cmd.Parameters.Add(param);
                        }
                        result = cmd.ExecuteNonQuery();
                    }
                }

                if (fireDataChange && result > 0)
                    RequestFireDataChangedByStatement(sqlNonQuery, result);
                return result;
            }

            return default(int);
        }

        public int ExecuteNonQueryWithParam(string sqlNonQuery, string[] paramNames, DbType[] paramTypes, object[] paramValues, int? commandTimeout = null) => ExecuteNonQueryWithParam(sqlNonQuery, paramNames, paramTypes, paramValues, true, commandTimeout: commandTimeout);

        private int ExecuteNonQueryWithParam(string sqlNonQuery, string[] paramNames, DbType[] paramTypes, object[] paramValues, bool fireDataChange, int? commandTimeout = null)
        {
            var N = 0;
            if (paramNames != null && paramValues != null && paramTypes != null)
            {
                N = Math.Min(paramNames.Length, paramValues.Length);
                N = Math.Min(N, paramTypes.Length);
                var result = 0;
                using (var con = CreateConnection())
                {
                    con.Open();
                    using (var cmd = CreateDbCommand(commandTimeout))
                    {
                        cmd.CommandText = sqlNonQuery;
                        cmd.Connection = con;

                        for (var i = 0; i < N; i++)
                        {
                            var param = CreateDbParameter();
                            param.ParameterName = paramNames[i];
                            param.DbType = paramTypes[i];
                            param.Value = paramValues[i] ?? DBNull.Value;
                            cmd.Parameters.Add(param);
                        }
                        result = cmd.ExecuteNonQuery();
                    }
                }

                if (fireDataChange && result > 0)
                    RequestFireDataChangedByStatement(sqlNonQuery, result);
                return result;
            }

            return default(int);
        }

        public int ExecuteSerialNonQueryWithParam(string sqlNonQuery, string[] paramNames, IEnumerable<object[]> paramValueSet, int? commandTimeout = null) => ExecuteSerialNonQueryWithParam(sqlNonQuery, paramNames, paramValueSet, true, commandTimeout: commandTimeout);

        private int ExecuteSerialNonQueryWithParam(string sqlNonQuery, string[] paramNames, IEnumerable<object[]> paramValueSet, bool fireDataChange, int? commandTimeout = null)
        {
            if (paramNames == null || paramValueSet == null || paramNames.Length == 0)
                return -1;

            var result = 0;
            using (var con = CreateConnection())
            {
                con.Open();
                using (var cmd = CreateDbCommand(commandTimeout))
                {
                    cmd.CommandText = sqlNonQuery;
                    cmd.Connection = con;

                    var parameters = new DbParameter[paramNames.Length];
                    var paramTypes = paramValueSet.First().Select(x => x == null ? null : x.GetType()).ToArray();
                    for (var i = 0; i < paramNames.Length; i++)
                    {
                        var param = CreateDbParameter(paramNames[i], paramTypes[i]);
                        cmd.Parameters.Add(param);
                        parameters[i] = param;
                    }

                    foreach (var values in paramValueSet)
                    {
                        for (var i = 0; i < paramNames.Length; i++)
                        {
                            parameters[i].Value = values[i] ?? DBNull.Value;
                        }
                        result += cmd.ExecuteNonQuery();
                    }
                }
            }

            if (fireDataChange && result > 0)
                RequestFireDataChangedByStatement(sqlNonQuery, result);
            return result;
        }

        public int ExecuteSerialNonQueryWithParam(string sqlNonQuery, string[] paramNames, DbType[] paramTypes, IEnumerable<object[]> paramValueSet, int? commandTimeout = null) => ExecuteSerialNonQueryWithParam(sqlNonQuery, paramNames, paramTypes, paramValueSet, true, commandTimeout: commandTimeout);

        private int ExecuteSerialNonQueryWithParam(string sqlNonQuery, string[] paramNames, DbType[] paramTypes, IEnumerable<object[]> paramValueSet, bool fireDataChange, int? commandTimeout = null)
        {
            var N = 0;
            if (paramNames != null && paramTypes != null && paramValueSet != null)
            {
                N = Math.Min(paramNames.Length, paramTypes.Length);
                var result = 0; using (var con = CreateConnection())
                {
                    con.Open();
                    using (var cmd = CreateDbCommand(commandTimeout))
                    {
                        cmd.CommandText = sqlNonQuery;
                        cmd.Connection = con;

                        var parameters = new DbParameter[paramNames.Length];
                        for (var i = 0; i < paramNames.Length; i++)
                        {
                            var param = CreateDbParameter();
                            param.ParameterName = paramNames[i];
                            param.DbType = paramTypes[i];
                            cmd.Parameters.Add(param);
                            parameters[i] = param;
                        }

                        foreach (var values in paramValueSet)
                        {
                            for (var i = 0; i < paramNames.Length; i++)
                            {
                                parameters[i].Value = values[i] ?? DBNull.Value;
                            }
                            result += cmd.ExecuteNonQuery();
                        }
                    }
                }

                if (result > 0)
                    RequestFireDataChangedByStatement(sqlNonQuery, result);
                return result;
            }

            return default(int);
        }

        public int ExecuteInsert(string table, IDictionary<string, object> fieldValues, int? commandTimeout = null)
        {
            var pairs = fieldValues.ToArray();
            return ExecuteInsert(table, pairs.Select(x => x.Key).ToArray(), pairs.Select(x => x.Value).ToArray(), commandTimeout: commandTimeout);
        }

        public int ExecuteInsert(string table, string[] fieldNames, object[] insertValues, int? commandTimeout = null)
        {
            var paramNames = AdoCommandHelper.CreateArray("@value{0}", fieldNames.Length);
            var result = ExecuteNonQueryWithParam(string.Format("INSERT INTO {0}({1}) VALUES({2});",
                QuoteIdentifier(table),
                string.Join(",", fieldNames.Select(x => QuoteIdentifier(x))),
                string.Join(",", paramNames)),
                paramNames, insertValues, false,
                commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = BuildInsertDataChange(table, fieldNames, new object[][] { insertValues });
                RequestFireDataChanged(dataChange);
            }

            return result;
        }

        public int ExecuteInsert(string table, string[] fieldNames, DbType[] fieldTypes, object[] insertValues, int? commandTimeout = null)
        {
            var paramNames = AdoCommandHelper.CreateArray("@value{0}", fieldNames.Length);
            var result = ExecuteNonQueryWithParam(string.Format("INSERT INTO {0}({1}) VALUES({2});",
                QuoteIdentifier(table),
                string.Join(",", fieldNames.Select(x => QuoteIdentifier(x))),
                string.Join(",", paramNames)),
                paramNames, fieldTypes, insertValues, false,
                commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = BuildInsertDataChange(table, fieldNames, new object[][] { insertValues });
                RequestFireDataChanged(dataChange);
            }

            return result;
        }

        public int ExecuteSerialInsert(string table, string[] fieldNames, IEnumerable<object[]> insertValueSets, int? commandTimeout = null)
        {
            var paramNames = AdoCommandHelper.CreateArray("@value{0}", fieldNames.Length);
            var result = ExecuteSerialNonQueryWithParam(string.Format("INSERT INTO {0}({1}) VALUES({2});",
                QuoteIdentifier(table),
                string.Join(",", fieldNames.Select(x => QuoteIdentifier(x))),
                string.Join(",", paramNames)),
                paramNames, insertValueSets, false,
                commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = BuildInsertDataChange(table, fieldNames, insertValueSets);
                RequestFireDataChanged(dataChange);
            }

            return result;
        }

        public int ExecuteSerialInsert(string table, string[] fieldNames, DbType[] fieldTypes, IEnumerable<object[]> insertValueSets, int? commandTimeout = null)
        {
            var paramNames = AdoCommandHelper.CreateArray("@value{0}", fieldNames.Length);
            var result = ExecuteSerialNonQueryWithParam(string.Format("INSERT INTO {0}({1}) VALUES({2});",
                QuoteIdentifier(table),
                string.Join(",", fieldNames.Select(x => QuoteIdentifier(x))),
                string.Join(",", paramNames)),
                paramNames, fieldTypes, insertValueSets, false,
                commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = BuildInsertDataChange(table, fieldNames, insertValueSets);
                RequestFireDataChanged(dataChange);
            }

            return result;
        }

        public int ExecuteUpdate(string table, string conditionField, object conditionValue, IDictionary<string, object> fieldValues, int? commandTimeout = null)
        {
            var pairs = fieldValues.ToArray();
            return ExecuteUpdate(table, new string[] { conditionField }, new object[] { conditionValue }, pairs.Select(x => x.Key).ToArray(), pairs.Select(x => x.Value).ToArray(), commandTimeout: commandTimeout);
        }

        public int ExecuteUpdate(string table, string conditionField, object conditionValue, string[] updateFieldNames, object[] updateFieldValues, int? commandTimeout = null) => ExecuteUpdate(table, new string[] { conditionField }, new object[] { conditionValue }, updateFieldNames, updateFieldValues, commandTimeout: commandTimeout);

        public int ExecuteUpdate(string table, string[] conditionFields, object[] conditionValues, string[] updateFieldNames, object[] updateFieldValues, int? commandTimeout = null)
        {
            if (conditionFields == null || conditionValues == null || updateFieldNames == null || updateFieldValues == null)
                throw new ArgumentNullException();
            if (conditionFields.Length != conditionValues.Length || updateFieldNames.Length != updateFieldValues.Length)
                throw new ArgumentException("Number of values is different from number of values.");

            var N = updateFieldNames.Length;
            var M = conditionFields.Length;

            var index = 0;
            var setPairs = string.Join(", ", updateFieldNames.Select(x => string.Format("{0} = @value{1}", QuoteIdentifier(x), index++)));
            var conditionPairs = string.Join(", ", conditionFields.Select(x => string.Format("{0} = @value{1}", QuoteIdentifier(x), index++)));

            var values = new List<object>();
            values.AddRange(updateFieldValues);
            values.AddRange(conditionValues);

            var result = ExecuteNonQueryWithParam(
                string.Format("UPDATE {0} SET {1} WHERE {2};", QuoteIdentifier(table), setPairs, conditionPairs),
                AdoCommandHelper.CreateArray("@value{0}", N + M),
                values.ToArray(), false,
                commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = BuildUpdateDataChange(table, conditionFields, conditionValues);
                RequestFireDataChanged(dataChange);
            }

            return result;
        }

        public int ExecuteDelete(string table, string conditionField, object conditionValue, int? commandTimeout = null)
        {
            var result = ExecuteNonQueryWithParam(string.Format("DELETE FROM {0} WHERE {1} = @value",
                QuoteIdentifier(table),
                QuoteIdentifier(conditionField)),
                new string[] { "@value" }, new object[] { conditionValue }, false,
                commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = BuildDeleteDataChange(table, conditionField, conditionValue);
                RequestFireDataChanged(dataChange);
            }

            return result;
        }

        public int ExecuteDeleteIn(string table, string conditionField, object[] conditionValues, int? commandTimeout = null)
        {
            var paramNames = AdoCommandHelper.CreateArray("@value{0}", conditionValues.Length);
            var result = ExecuteNonQueryWithParam(string.Format("DELETE FROM {0} WHERE {1} IN ({2});",
                QuoteIdentifier(table),
                QuoteIdentifier(conditionField), string.Join(",", paramNames)),
                paramNames, conditionValues, false,
                commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = BuildDeleteDataChange(table, conditionField, conditionValues);
                RequestFireDataChanged(dataChange);
            }

            return result;
        }

        public int ExecuteDelete(string table, string[] conditionFields, object[] conditionValues, int? commandTimeout = null)
        {
            var index = 0;
            var paramNames = AdoCommandHelper.CreateArray("@condition{0}", conditionFields.Length);

            var result = ExecuteNonQueryWithParam(string.Format("DELETE FROM {0} WHERE {1};",
                QuoteIdentifier(table)
                , string.Join(" AND ", conditionFields.Select(x => string.Format("{0} = @condition{1}", QuoteIdentifier(x), index++)))),
                paramNames, conditionValues, false,
                commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = BuildDeleteDataChange(table, conditionFields, conditionValues);
                RequestFireDataChanged(dataChange);
            }

            return result;
        }

        public int ExecuteDelete(string table, int? commandTimeout = null)
        {
            var result = ExecuteNonQuery(string.Format("DELETE FROM {0};", QuoteIdentifier(table)), false, commandTimeout: commandTimeout);

            if (result > 0)
            {
                var dataChange = new DataChange
                {
                    ChangeInfo = table,
                    ChangedTableGetter = args => new string[] { (string)args },
                };
                RequestFireDataChanged(dataChange);
            }

            return result;
        }
    }
}