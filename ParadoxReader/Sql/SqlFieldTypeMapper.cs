using System;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Shared mapping from a Paradox field's on-disk type to the CLR type
    /// exposed via IDataReader.GetFieldType, used by both <see cref="SqlDataReader"/>
    /// (single-table projections) and <see cref="JoinedSqlDataReader"/> (JOIN
    /// projections spanning multiple tables).
    /// </summary>
    internal static class SqlFieldTypeMapper
    {
        public static Type GetFieldType(ParadoxFile.FieldInfo fInfo)
        {
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
    }
}
