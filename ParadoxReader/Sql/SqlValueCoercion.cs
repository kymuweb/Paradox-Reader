using System;
using System.Collections.Generic;
using System.Globalization;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Converts a parsed <see cref="SqlValue"/> literal into the CLR
    /// representation expected by <see cref="ParadoxRecord"/>/<see cref="ParadoxTableFile"/>
    /// for a given field, mirroring the type mapping used by
    /// <see cref="ParadoxDataReader.GetFieldType"/> and <see cref="ParadoxRecord"/>'s
    /// internal WriteField/ReadField logic.
    /// </summary>
    internal static class SqlValueCoercion
    {
        /// <summary>
        /// Converts a parsed <see cref="SqlValue"/> literal (or, for
        /// <see cref="SqlValue.Kind.Parameter"/>, a bound parameter's raw CLR
        /// value looked up by name in <paramref name="parameters"/>) into the
        /// CLR representation expected by <see cref="ParadoxRecord"/>/
        /// <see cref="ParadoxTableFile"/> for a given field.
        /// </summary>
        public static object Coerce(SqlValue value, ParadoxFile.FieldInfo field,
            IDictionary<string, object> parameters, object existingValue = null)
        {
            if (value.ValueKind == SqlValue.Kind.Parameter)
            {
                if (parameters == null || !parameters.TryGetValue(value.ParameterName, out var paramValue))
                    throw new SqlExecutionException(
                        $"No value supplied for parameter '@{value.ParameterName}'; add a matching IDbDataParameter to the command's Parameters collection.");
                return CoerceRawValue(paramValue, field, existingValue);
            }

            return Coerce(value, field, existingValue);
        }

        public static object Coerce(SqlValue value, ParadoxFile.FieldInfo field, object existingValue = null)
        {
            if (value.ValueKind == SqlValue.Kind.Parameter)
                throw new SqlExecutionException(
                    $"Parameter '@{value.ParameterName}' encountered without a parameter value dictionary; this statement requires parameter binding.");

            if (value.ValueKind == SqlValue.Kind.Null)
                return null;

            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                    return RequireString(value, field.fType);

                case ParadoxFieldTypes.Short:
                    return checked((short)RequireNumber(value, field.fType));

                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                    return checked((int)RequireNumber(value, field.fType));

                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    return RequireNumber(value, field.fType);

                case ParadoxFieldTypes.BCD:
                    return (decimal)RequireNumber(value, field.fType);

                case ParadoxFieldTypes.Date:
                case ParadoxFieldTypes.Timestamp:
                    return ParseDate(value, field.fType);

                case ParadoxFieldTypes.Time:
                    return ParseTime(value, field.fType);

                case ParadoxFieldTypes.Logical:
                    return RequireBool(value, field.fType);

                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                {
                    string text = RequireString(value, field.fType);
                    // Preserve BlobInfo from the existing value when updating a memo in
                    // place, matching ParadoxRecord.WriteField's expectation that
                    // MemoValue.BlobInfo survives the read-modify-write cycle.
                    byte[] blobInfo = (existingValue as MemoValue)?.BlobInfo;
                    return new MemoValue(text, blobInfo);
                }

                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                case ParadoxFieldTypes.Bytes:
                    throw new SqlExecutionException(
                        "Binary field types (BLOb/OLE/Graphic/Bytes) cannot be set via SQL literals in this engine.");

                default:
                    throw new SqlExecutionException($"Unsupported field type '{field.fType}' for value coercion.");
            }
        }

        /// <summary>
        /// Converts a raw bound parameter value (as supplied via
        /// <see cref="System.Data.IDbDataParameter.Value"/>, e.g. from a
        /// caller's own CLR types such as int/string/DateTime/bool/decimal)
        /// into the representation expected for <paramref name="field"/>.
        /// Unlike <see cref="Coerce(SqlValue, ParadoxFile.FieldInfo, object)"/>,
        /// which only has to parse SQL text literals, this accepts values
        /// that are frequently already the exact target CLR type (common when
        /// binding from a generic data layer), converting only when needed.
        /// </summary>
        private static object CoerceRawValue(object rawValue, ParadoxFile.FieldInfo field, object existingValue)
        {
            if (rawValue == null || rawValue == DBNull.Value)
                return null;

            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                    return Convert.ToString(rawValue, CultureInfo.InvariantCulture);

                case ParadoxFieldTypes.Short:
                    return Convert.ToInt16(rawValue, CultureInfo.InvariantCulture);

                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                    return Convert.ToInt32(rawValue, CultureInfo.InvariantCulture);

                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    return Convert.ToDouble(rawValue, CultureInfo.InvariantCulture);

                case ParadoxFieldTypes.BCD:
                    return Convert.ToDecimal(rawValue, CultureInfo.InvariantCulture);

                case ParadoxFieldTypes.Date:
                case ParadoxFieldTypes.Timestamp:
                    return rawValue is DateTime dt ? dt : Convert.ToDateTime(rawValue, CultureInfo.InvariantCulture);

                case ParadoxFieldTypes.Time:
                    if (rawValue is TimeSpan ts) return ts;
                    if (rawValue is DateTime timeDt) return timeDt.TimeOfDay;
                    if (rawValue is string timeStr && TryParseTimeSpanInvariant(timeStr, out var parsedTs))
                        return parsedTs;
                    throw new SqlExecutionException($"Could not convert parameter value '{rawValue}' to a TimeSpan for field type '{field.fType}'.");

                case ParadoxFieldTypes.Logical:
                    return Convert.ToBoolean(rawValue, CultureInfo.InvariantCulture);

                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                {
                    string text = rawValue as string ?? Convert.ToString(rawValue, CultureInfo.InvariantCulture);
                    byte[] blobInfo = (existingValue as MemoValue)?.BlobInfo;
                    return new MemoValue(text, blobInfo);
                }

                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                case ParadoxFieldTypes.Bytes:
                    if (rawValue is byte[] bytes) return bytes;
                    throw new SqlExecutionException(
                        $"Binary field types (BLOb/OLE/Graphic/Bytes) require a byte[] parameter value for field type '{field.fType}'.");

                default:
                    throw new SqlExecutionException($"Unsupported field type '{field.fType}' for parameter value coercion.");
            }
        }

        private static string RequireString(SqlValue value, ParadoxFieldTypes fType)
        {
            if (value.ValueKind == SqlValue.Kind.String) return value.StringValue;
            if (value.ValueKind == SqlValue.Kind.Number)
                return value.NumberValue.ToString(CultureInfo.InvariantCulture);
            throw new SqlExecutionException($"Expected a string literal for field type '{fType}'.");
        }

        private static double RequireNumber(SqlValue value, ParadoxFieldTypes fType)
        {
            if (value.ValueKind == SqlValue.Kind.Number) return value.NumberValue;
            if (value.ValueKind == SqlValue.Kind.String &&
                double.TryParse(value.StringValue, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                return d;
            throw new SqlExecutionException($"Expected a numeric literal for field type '{fType}'.");
        }

        private static bool RequireBool(SqlValue value, ParadoxFieldTypes fType)
        {
            if (value.ValueKind == SqlValue.Kind.Bool) return value.BoolValue;
            if (value.ValueKind == SqlValue.Kind.String)
            {
                if (string.Equals(value.StringValue, "TRUE", StringComparison.OrdinalIgnoreCase)) return true;
                if (string.Equals(value.StringValue, "FALSE", StringComparison.OrdinalIgnoreCase)) return false;
            }
            throw new SqlExecutionException($"Expected TRUE/FALSE for field type '{fType}'.");
        }

        private static readonly string[] DateFormats =
        {
            "MM/dd/yyyy", "yyyy-MM-dd", "MM/dd/yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss"
        };

        private static DateTime ParseDate(SqlValue value, ParadoxFieldTypes fType)
        {
            if (value.ValueKind != SqlValue.Kind.String)
                throw new SqlExecutionException($"Expected a quoted date literal for field type '{fType}'.");

            if (DateTime.TryParseExact(value.StringValue, DateFormats, CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out var dt))
                return dt;

            if (DateTime.TryParse(value.StringValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
                return dt;

            throw new SqlExecutionException($"Could not parse date literal '{value.StringValue}' for field type '{fType}'.");
        }

        private static TimeSpan ParseTime(SqlValue value, ParadoxFieldTypes fType)
        {
            if (value.ValueKind != SqlValue.Kind.String)
                throw new SqlExecutionException($"Expected a quoted time literal for field type '{fType}'.");

            if (TryParseTimeSpanInvariant(value.StringValue, out var ts))
                return ts;

            throw new SqlExecutionException($"Could not parse time literal '{value.StringValue}' for field type '{fType}'.");
        }

        /// <summary>
        /// .NET 3.5-compatible replacement for the .NET 4.0+
        /// <c>TimeSpan.TryParse(string, IFormatProvider, out TimeSpan)</c> and
        /// <c>TimeSpan.TryParseExact(...)</c> overloads, which are not
        /// available on this target framework. Accepts "hh:mm:ss" (with an
        /// optional leading '-' and optional fractional seconds) explicitly,
        /// then falls back to the culture-invariant thread-independent
        /// <see cref="TimeSpan.Parse(string)"/> by temporarily switching the
        /// current culture.
        /// </summary>
        private static bool TryParseTimeSpanInvariant(string text, out TimeSpan result)
        {
            result = TimeSpan.Zero;
            if (string.IsNullOrEmpty(text)) return false;

            var savedCulture = System.Threading.Thread.CurrentThread.CurrentCulture;
            try
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
                try
                {
                    result = TimeSpan.Parse(text);
                    return true;
                }
                catch (FormatException)
                {
                    return false;
                }
                catch (OverflowException)
                {
                    return false;
                }
            }
            finally
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = savedCulture;
            }
        }
    }

    public class SqlExecutionException : Exception
    {
        public SqlExecutionException(string message) : base(message) { }
        public SqlExecutionException(string message, Exception inner) : base(message, inner) { }
    }
}
