using System;
using System.Data;
using System.Text.RegularExpressions;

namespace Kraken.Ado.Command
{
    public static class AdoCommandHelper
    {
        public static DbType ConvertToDbType(DataColumn column) => ConvertToDbType(column.DataType);

        public static DbType ConvertToDbType(object value) => ConvertToDbType(value.GetType());

        public static DbType ConvertToDbType(Type type)
        {
            if (type == typeof(bool))
                return DbType.Boolean;

            if (type == typeof(byte))
                return DbType.Byte;

            if (type == typeof(int))
                return DbType.Int32;

            if (type == typeof(uint))
                return DbType.UInt32;

            if (type == typeof(long))
                return DbType.Int64;

            if (type == typeof(ulong))
                return DbType.UInt64;

            if (type == typeof(float))
                return DbType.Single;

            if (type == typeof(double))
                return DbType.Double;

            if (type == typeof(DateTime))
                return DbType.DateTime;

            if (type == typeof(Guid))
                return DbType.Guid;

            if (type == typeof(decimal))
                return DbType.Decimal;

            if (type == typeof(string))
                return DbType.String;

            if (type == typeof(byte[]))
                return DbType.Binary;

            return DbType.String;
        }

        public static object CastValueForDbType(object value, DbType dbType)
        {
            if (value is DBNull)
                return null;

#pragma warning disable CCN0001 // Non exhaustive patterns in switch block
            switch (dbType)
            {
                case DbType.Int32:
                    {
                        if (value is short)
                            return (int)(short)value;
                        if (value is byte)
                            return (int)(byte)value;
                        if (value is long)
                            return (int)(long)value;
                        if (value is decimal)
                            return (int)(decimal)value;
                        break;
                    }
                case DbType.Int16:
                    {
                        if (value is int)
                            return (short)(int)value;
                        if (value is byte)
                            return (short)(byte)value;
                        if (value is long)
                            return (short)(long)value;
                        if (value is decimal)
                            return (short)(decimal)value;
                        break;
                    }
                case DbType.Int64:
                    {
                        if (value is int)
                            return (long)(int)value;
                        if (value is short)
                            return (long)(short)value;
                        if (value is byte)
                            return (long)(byte)value;
                        if (value is decimal)
                            return (long)(decimal)value;
                        break;
                    }
                case DbType.Single:
                    {
                        if (value is double)
                            return (float)(double)value;
                        if (value is decimal)
                            return (float)(decimal)value;
                        break;
                    }
                case DbType.Double:
                    {
                        if (value is float)
                            return (double)(float)value;
                        if (value is double)
                            return (double)value;
                        if (value is decimal)
                            return (double)(decimal)value;
                        break;
                    }
                case DbType.Decimal:
                    {
                        if (value is decimal)
                            return (decimal)value;
                        if (value is float)
                            return (decimal)(float)value;
                        if (value is double)
                            return (decimal)(double)value;
                        break;
                    }
                case DbType.String:
                    {
                        if (value is Guid)
                            return ((Guid)value).ToString();
                        break;
                    }
                case DbType.Guid:
                    {
                        if (value is string)
                        {
                            var result = Guid.Empty;
                            Guid.TryParse((string)value, out result);
                            return result;
                        }
                        if (value is byte[])
                            return ParseBlobAsGuid((byte[])value);

                        break;
                    }
                case DbType.Binary:
                case DbType.Boolean:
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                    break;

                default:
                    throw new ArgumentException("Illegal database type [" + Enum.GetName(typeof(DbType), dbType) + "]");
            }
#pragma warning restore CCN0001 // Non exhaustive patterns in switch block

            return value;
        }

        private static Guid ParseBlobAsGuid(byte[] blob)
        {
            var data = blob;
            if (blob.Length > 16)
            {
                data = new byte[16];
                for (var i = 0; i < 16; i++)
                    data[i] = blob[i];
            }
            else if (blob.Length < 16)
            {
                data = new byte[16];
                for (var i = 0; i < blob.Length; i++)
                    data[i] = blob[i];
            }

            return new Guid(data);
        }

        public static string[] CreateArray(string pattern, int numberOfElement)
        {
            var paramNames = new string[numberOfElement];
            for (var i = 0; i < paramNames.Length; i++)
                paramNames[i] = string.Format(pattern, i);
            return paramNames;
        }

        private static readonly Regex _RxIsSqlDDLStatement = new Regex(@"^(\s)*(?<command>(INSERT(\s)+(INTO(\s)+)?)|(UPDATE(\s)+)|(DELETE(\s)+(FROM(\s)+)?))\W?(?<table>\w+)\W?", RegexOptions.IgnoreCase);

        public static bool IsSqlDDLStatement(string sqlStatement, out int command, out string table)
        {
            command = 0;
            table = null;
            var match = _RxIsSqlDDLStatement.Match(sqlStatement);
            if (match.Success)
            {
                var sCommand = match.Groups["command"].Value.ToUpper();
                if (sCommand.StartsWith("INSERT", StringComparison.Ordinal))
                    command = 1;
                else if (sCommand.StartsWith("UPDATE", StringComparison.Ordinal))
                    command = 2;
                else if (sCommand.StartsWith("DELETE", StringComparison.Ordinal))
                    command = 3;

                table = match.Groups["table"].Value;
            }
            return match.Success;
        }
    }
}