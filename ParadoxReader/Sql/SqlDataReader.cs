using System;
using System.Collections.Generic;
using System.Data;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// IDataReader over a sequence of ParadoxRecord, projecting only the
    /// requested column indices (or all columns for "SELECT *"). Column
    /// metadata (names/types) comes from the parent ParadoxFile, but this
    /// reader's ordinal positions match the projection order, not the
    /// underlying table's field order.
    /// </summary>
    internal sealed class SqlDataReader : IDataReader
    {
        private readonly ParadoxFile file;
        private readonly int[] columnIndices; // maps projected ordinal -> table field index
        private readonly string[] columnNames;
        private readonly IEnumerator<ParadoxRecord> enumerator;

        public ParadoxRecord CurrentRecord => enumerator.Current;

        public SqlDataReader(ParadoxFile file, IEnumerable<ParadoxRecord> rows, int[] columnIndices)
        {
            this.file = file;
            this.columnIndices = columnIndices;
            this.columnNames = new string[columnIndices.Length];
            for (int i = 0; i < columnIndices.Length; i++)
                this.columnNames[i] = file.FieldNames[columnIndices[i]];
            this.enumerator = rows.GetEnumerator();
        }

        public int FieldCount => columnIndices.Length;

        public string GetName(int i) => columnNames[i];

        public int GetOrdinal(string name)
        {
            for (int i = 0; i < columnNames.Length; i++)
                if (string.Equals(columnNames[i], name, StringComparison.OrdinalIgnoreCase))
                    return i;
            return -1;
        }

        public object GetValue(int i) => CurrentRecord.DataValues[columnIndices[i]];

        public Type GetFieldType(int i)
        {
            // Delegate to a throwaway ParadoxDataReader-style mapping via the
            // underlying field's fType, matching ParadoxDataReader.GetFieldType.
            var fInfo = file.FieldTypes[columnIndices[i]];
            switch (fInfo.fType)
            {
                case ParadoxFieldTypes.Alpha:
                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                    return typeof(string);
                case ParadoxFieldTypes.Short:
                    return typeof(short);
                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                    return typeof(int);
                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    return typeof(double);
                case ParadoxFieldTypes.BCD:
                    return typeof(decimal);
                case ParadoxFieldTypes.Date:
                case ParadoxFieldTypes.Timestamp:
                    return typeof(DateTime);
                case ParadoxFieldTypes.Time:
                    return typeof(TimeSpan);
                case ParadoxFieldTypes.Logical:
                    return typeof(bool);
                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                case ParadoxFieldTypes.Bytes:
                    return typeof(byte[]);
                default:
                    throw new NotSupportedException();
            }
        }

        public string GetDataTypeName(int i) => "pxf" + file.FieldTypes[columnIndices[i]].fType;

        public bool GetBoolean(int i) => (bool)GetValue(i);
        public byte GetByte(int i) => (byte)GetValue(i);
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
        public char GetChar(int i) => throw new NotImplementedException();
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
        public Guid GetGuid(int i) => throw new NotImplementedException();
        public short GetInt16(int i) => (short)GetValue(i);
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => (long)GetValue(i);
        public float GetFloat(int i) => (float)GetValue(i);
        public double GetDouble(int i) => (double)GetValue(i);
        public string GetString(int i) => (string)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public IDataReader GetData(int i) => throw new NotImplementedException();
        public bool IsDBNull(int i) => GetValue(i) == null || GetValue(i) == DBNull.Value;
        public int GetValues(object[] values)
        {
            int n = Math.Min(values.Length, FieldCount);
            for (int i = 0; i < n; i++) values[i] = GetValue(i) ?? DBNull.Value;
            return n;
        }

        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));

        public void Close() { }
        public DataTable GetSchemaTable()
        {
            // Standard ADO.NET schema table shape (subset of columns commonly
            // consumed by DataTable.Load(IDataReader) and generic data-bound
            // UI controls that call IDataReader.GetSchemaTable()).
            var schema = new DataTable("SchemaTable");
            schema.Columns.Add("ColumnName", typeof(string));
            schema.Columns.Add("ColumnOrdinal", typeof(int));
            schema.Columns.Add("ColumnSize", typeof(int));
            schema.Columns.Add("DataType", typeof(Type));
            schema.Columns.Add("AllowDBNull", typeof(bool));
            schema.Columns.Add("IsReadOnly", typeof(bool));
            schema.Columns.Add("IsKey", typeof(bool));
            schema.Columns.Add("IsAutoIncrement", typeof(bool));

            for (int i = 0; i < columnIndices.Length; i++)
            {
                var fieldInfo = file.FieldTypes[columnIndices[i]];
                var row = schema.NewRow();
                row["ColumnName"] = columnNames[i];
                row["ColumnOrdinal"] = i;
                row["ColumnSize"] = fieldInfo.fSize;
                row["DataType"] = GetFieldType(i);
                row["AllowDBNull"] = true; // Paradox fields are nullable except where enforced by validity checks not tracked here
                row["IsReadOnly"] = false;
                row["IsKey"] = columnIndices[i] < file.primaryKeyFields;
                row["IsAutoIncrement"] = fieldInfo.fType == ParadoxFieldTypes.AutoInc;
                schema.Rows.Add(row);
            }

            return schema;
        }
        public bool NextResult() => false;
        public bool Read() => enumerator.MoveNext();

        public int Depth => 0;
        public bool IsClosed => false;
        public int RecordsAffected => -1;

        public void Dispose() { }
    }
}
