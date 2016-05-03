using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace ParadoxReader
{
    public class ParadoxDataReader : IDataReader
    {
        public ParadoxFile File { get; private set; }

        private IEnumerator<ParadoxRecord> enumerator;

        public ParadoxRecord CurrentRecord
        {
            get { return this.enumerator.Current; }
        }

        public ParadoxDataReader(ParadoxFile file, IEnumerable<ParadoxRecord> query)
        {
            this.File = file;
            this.enumerator = query.GetEnumerator();
        }

        public void Dispose()
        {
        }

        public string GetName(int i)
        {
            return this.File.FieldNames[i];
        }

        public string GetDataTypeName(int colIndex)
        {
            return "pxf" + this.File.FieldTypes[colIndex].fType;
        }

        public Type GetFieldType(int colIndex)
        {
            var fInfo = this.File.FieldTypes[colIndex];
            switch (fInfo.fType)
            {
                case ParadoxFieldTypes.Alpha:
                case ParadoxFieldTypes.MemoBLOb:
                    return typeof (string);
                case ParadoxFieldTypes.Short:
                    return typeof(short);
                case ParadoxFieldTypes.Long:
                    return typeof(uint);
                case ParadoxFieldTypes.Currency:
                    return typeof(double);
                case ParadoxFieldTypes.Number:
                    return typeof(double);
                case ParadoxFieldTypes.Date:
                    return typeof(DateTime);
                case ParadoxFieldTypes.Timestamp:
                    return typeof(DateTime);
                default:
                    throw new NotSupportedException();
            }
        }

        public object GetValue(int i)
        {
            return this.CurrentRecord.DataValues[i];
        }

        public int GetValues(object[] values)
        {
            return 0;
        }

        public int GetOrdinal(string name)
        {
            return Array.FindIndex(this.File.FieldNames,
                                   f => string.Equals(f, name, StringComparison.InvariantCultureIgnoreCase));
        }

        public bool GetBoolean(int i)
        {
            return (bool)this.GetValue(i);
        }

        public byte GetByte(int i)
        {
            return (byte)this.GetValue(i);
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            return (short)this.GetValue(i);
        }

        public int GetInt32(int i)
        {
            return (int)this.GetValue(i);
        }

        public long GetInt64(int i)
        {
            return (long)this.GetValue(i);
        }

        public float GetFloat(int i)
        {
            return (float)this.GetValue(i);
        }

        public double GetDouble(int i)
        {
            return (double)this.GetValue(i);
        }

        public string GetString(int i)
        {
            return (string)this.GetValue(i);
        }

        public decimal GetDecimal(int i)
        {
            return (decimal)this.GetValue(i);
        }

        public DateTime GetDateTime(int i)
        {
            return (DateTime)this.GetValue(i);
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            return this.GetValue(i) == DBNull.Value;
        }

        public int FieldCount
        {
            get { return this.File.FieldCount; }
        }

        public object this[int i]
        {
            get { return this.GetValue(i); }
        }

        public object this[string name]
        {
            get { return this.GetValue(this.GetOrdinal(name)); }
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            return this.enumerator.MoveNext();
        }

        public int Depth
        {
            get { return 0; }
        }

        public bool IsClosed
        {
            get { return false; }
        }

        public int RecordsAffected
        {
            get { return 0; }
        }
    }
}
