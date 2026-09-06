using System;
using System.Data;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Minimal <see cref="IDbDataParameter"/> implementation for
    /// <see cref="ParadoxCommand"/>. Values are bound by name (e.g. "@id" in
    /// <see cref="ParadoxCommand.CommandText"/> matches a parameter whose
    /// <see cref="ParameterName"/> is "id" or "@id") or, for positional '?'
    /// placeholders, by occurrence order within the statement.
    /// </summary>
    public sealed class ParadoxParameter : IDbDataParameter
    {
        public ParadoxParameter() { }

        public ParadoxParameter(string parameterName, object value)
        {
            ParameterName = parameterName;
            Value = value;
        }

        public DbType DbType { get; set; }
        public ParameterDirection Direction { get; set; } = ParameterDirection.Input;
        public bool IsNullable { get; set; } = true;
        public string ParameterName { get; set; }
        public string SourceColumn { get; set; }
        public DataRowVersion SourceVersion { get; set; }
        public object Value { get; set; }
        public byte Precision { get; set; }
        public byte Scale { get; set; }
        public int Size { get; set; }

        /// <summary>
        /// The parameter name with any leading '@'/':' prefix stripped, used
        /// to match against <see cref="SqlValue.ParameterName"/> tokens
        /// parsed from CommandText.
        /// </summary>
        internal string NormalizedName =>
            string.IsNullOrEmpty(ParameterName) ? ParameterName
                : (ParameterName[0] == '@' || ParameterName[0] == ':' ? ParameterName.Substring(1) : ParameterName);
    }
}
