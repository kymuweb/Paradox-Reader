using System.Collections.Generic;

namespace ParadoxReader.Sql
{
    // ----------------------------------------------------------------
    // Top-level statements
    // ----------------------------------------------------------------

    internal abstract class SqlStatement { }

    internal sealed class SelectStatement : SqlStatement
    {
        /// <summary>Column names to project, or null/empty for "SELECT *".</summary>
        public List<string> Columns;
        public bool IsSelectStar;
        public TableRef Table;
        public WhereExpr Where;
    }

    internal sealed class InsertStatement : SqlStatement
    {
        public TableRef Table;
        /// <summary>Explicit column list, or null to mean "all fields in table order".</summary>
        public List<string> Columns;
        public List<SqlValue> Values;
    }

    internal sealed class UpdateStatement : SqlStatement
    {
        public TableRef Table;
        public List<SqlAssignment> Assignments;
        public WhereExpr Where;
    }

    internal sealed class DeleteStatement : SqlStatement
    {
        public TableRef Table;
        public WhereExpr Where;
    }

    // ----------------------------------------------------------------
    // Supporting nodes
    // ----------------------------------------------------------------

    internal sealed class TableRef
    {
        /// <summary>Raw path/table token as written in the SQL text (quoted literal or bare identifier).</summary>
        public string Path;
        /// <summary>Optional alias, e.g. the "T" in "'table.db' T".</summary>
        public string Alias;
    }

    internal sealed class SqlAssignment
    {
        public string ColumnName;
        public SqlValue Value;
    }

    /// <summary>A literal value parsed from SQL text, pre-coercion to a field's Paradox type.</summary>
    internal sealed class SqlValue
    {
        public enum Kind { String, Number, Bool, Null, Parameter }
        public Kind ValueKind;
        public string StringValue;
        public double NumberValue;
        public bool BoolValue;

        /// <summary>
        /// For Kind.Parameter: the parameter's name (without leading '@'), or
        /// null for a positional '?' placeholder. Positional parameters are
        /// matched to <see cref="ParadoxCommand.Parameters"/> by occurrence
        /// order within the statement.
        /// </summary>
        public string ParameterName;

        public static SqlValue OfString(string s) => new SqlValue { ValueKind = Kind.String, StringValue = s };
        public static SqlValue OfNumber(double d) => new SqlValue { ValueKind = Kind.Number, NumberValue = d };
        public static SqlValue OfBool(bool b) => new SqlValue { ValueKind = Kind.Bool, BoolValue = b };
        public static SqlValue OfNull() => new SqlValue { ValueKind = Kind.Null };
        public static SqlValue OfParameter(string name) => new SqlValue { ValueKind = Kind.Parameter, ParameterName = name };
    }

    // ----------------------------------------------------------------
    // WHERE expression tree
    // ----------------------------------------------------------------

    internal abstract class WhereExpr { }

    internal sealed class WhereComparison : WhereExpr
    {
        public string ColumnName;
        public ParadoxCompareOperator Operator;
        public SqlValue Value;
    }

    internal sealed class WhereAnd : WhereExpr
    {
        public WhereExpr Left;
        public WhereExpr Right;
    }

    internal sealed class WhereOr : WhereExpr
    {
        public WhereExpr Left;
        public WhereExpr Right;
    }
}
