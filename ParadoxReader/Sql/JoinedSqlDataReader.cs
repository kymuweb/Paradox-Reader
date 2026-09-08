using System;
using System.Collections.Generic;
using System.Data;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// IDataReader over the projected results of a multi-table SELECT with
    /// one or more JOINs. Each result row is an array of <see cref="ParadoxRecord"/>
    /// (or null, for the "unmatched" side of a LEFT JOIN), one per table in
    /// FROM/JOIN order; the projected column list maps each output ordinal to
    /// a (table index, field index) pair.
    /// </summary>
    internal sealed class JoinedSqlDataReader : IDataReader
    {
        /// <summary>Per-table field metadata, in FROM/JOIN order.</summary>
        private readonly ParadoxFile[] tables;

        /// <summary>For each projected output column: which table (index into <see cref="tables"/>) and field index within it.</summary>
        private readonly int[] projectionTableIndex;
        private readonly int[] projectionFieldIndex;
        private readonly string[] columnNames;

        private readonly IEnumerator<ParadoxRecord[]> enumerator;

        public JoinedSqlDataReader(ParadoxFile[] tables, IEnumerable<ParadoxRecord[]> rows,
            int[] projectionTableIndex, int[] projectionFieldIndex, string[] columnNames)
        {
            this.tables = tables;
            this.projectionTableIndex = projectionTableIndex;
            this.projectionFieldIndex = projectionFieldIndex;
            this.columnNames = columnNames;
            this.enumerator = rows.GetEnumerator();
        }

        private ParadoxRecord[] CurrentRow => enumerator.Current;

        public int FieldCount => projectionTableIndex.Length;

        public string GetName(int i) => columnNames[i];

        public int GetOrdinal(string name)
        {
            for (int i = 0; i < columnNames.Length; i++)
                if (string.Equals(columnNames[i], name, StringComparison.OrdinalIgnoreCase))
                    return i;
            return -1;
        }

        public object GetValue(int i)
        {
            var rec = CurrentRow[projectionTableIndex[i]];
            if (rec == null) return null; // unmatched side of a LEFT JOIN
            return rec.DataValues[projectionFieldIndex[i]];
        }

        public Type GetFieldType(int i) => SqlFieldTypeMapper.GetFieldType(tables[projectionTableIndex[i]].FieldTypes[projectionFieldIndex[i]]);

        public string GetDataTypeName(int i) => "pxf" + tables[projectionTableIndex[i]].FieldTypes[projectionFieldIndex[i]].fType;

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
            var schema = new DataTable("SchemaTable");
            schema.Columns.Add("ColumnName", typeof(string));
            schema.Columns.Add("ColumnOrdinal", typeof(int));
            schema.Columns.Add("ColumnSize", typeof(int));
            schema.Columns.Add("DataType", typeof(Type));
            schema.Columns.Add("AllowDBNull", typeof(bool));
            schema.Columns.Add("IsReadOnly", typeof(bool));
            schema.Columns.Add("IsKey", typeof(bool));
            schema.Columns.Add("IsAutoIncrement", typeof(bool));

            for (int i = 0; i < projectionTableIndex.Length; i++)
            {
                var table = tables[projectionTableIndex[i]];
                var fieldIndex = projectionFieldIndex[i];
                var fieldInfo = table.FieldTypes[fieldIndex];
                var row = schema.NewRow();
                row["ColumnName"] = columnNames[i];
                row["ColumnOrdinal"] = i;
                row["ColumnSize"] = fieldInfo.fSize;
                row["DataType"] = GetFieldType(i);
                row["AllowDBNull"] = true;
                row["IsReadOnly"] = false;
                row["IsKey"] = fieldIndex < table.primaryKeyFields;
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
