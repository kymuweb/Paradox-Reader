using System.Collections.Generic;

namespace ParadoxReader.Sql
{
    // ----------------------------------------------------------------
    // Top-level statements
    // ----------------------------------------------------------------

    internal abstract class SqlStatement { }

    internal sealed class SelectStatement : SqlStatement
    {
        /// <summary>
        /// Columns to project, or null/empty for a bare "SELECT *" (see
        /// <see cref="IsSelectStar"/>). Individual entries may themselves be a
        /// star (optionally alias-qualified, e.g. "A.*") which is expanded to
        /// that table's full field list at execution time.
        /// </summary>
        public List<SqlColumnRef> Columns;
        /// <summary>True for a bare "SELECT *" (no alias prefix), before any FROM/JOIN table aliases are known.</summary>
        public bool IsSelectStar;
        public TableRef Table;
        /// <summary>Additional tables joined to <see cref="Table"/>, in left-to-right order. Null/empty for a single-table SELECT.</summary>
        public List<JoinClause> Joins;
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

    /// <summary>
    /// A (possibly alias-qualified) column reference, e.g. "A.ID", "ID", or
    /// "A.*" (ColumnName == "*"). Used in the SELECT column list and JOIN ON
    /// clauses, where the alias must be preserved to disambiguate columns
    /// across multiple joined tables.
    /// </summary>
    internal sealed class SqlColumnRef
    {
        public string TableAlias;
        public string ColumnName;
        public bool IsStar => ColumnName == "*";
        public override string ToString() => TableAlias == null ? ColumnName : $"{TableAlias}.{ColumnName}";
    }

    internal enum JoinType { Inner, Left }

    /// <summary>An equality condition ("alias.col = alias.col") within a JOIN's ON clause.</summary>
    internal sealed class JoinEquality
    {
        public SqlColumnRef Left;
        public SqlColumnRef Right;
    }

    internal sealed class JoinClause
    {
        public JoinType Type;
        public TableRef Table;
        /// <summary>AND-combined equality conditions from the ON clause (at least one).</summary>
        public List<JoinEquality> Conditions;
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
        /// <summary>Optional table alias prefix (e.g. the "A" in "A.ID = ..."), used to disambiguate columns across joined tables. Null for unqualified references.</summary>
        public string TableAlias;
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
