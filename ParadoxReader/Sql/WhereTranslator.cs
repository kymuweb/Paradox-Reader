using System;
using System.Collections.Generic;
using System.Linq;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Translates a parsed <see cref="WhereExpr"/> tree into a
    /// <see cref="Predicate{ParadoxRecord}"/> usable with <see cref="ParadoxFile.Enumerate"/>,
    /// and (when the WHERE clause is a single equality on an indexed field)
    /// into a <see cref="ParadoxCondition"/> usable with
    /// <see cref="ParadoxPrimaryKey.Enumerate"/> / <see cref="SecondaryIndexHandle.Enumerate"/>
    /// for index-accelerated execution.
    /// </summary>
    internal static class WhereTranslator
    {
        /// <summary>
        /// Builds a full-table-scan predicate for the given WHERE tree, resolving
        /// column names against the table's field list and coercing literals to
        /// each field's native CLR representation.
        /// </summary>
        public static Predicate<ParadoxRecord> ToPredicate(WhereExpr expr, ParadoxFile table,
            IDictionary<string, object> parameters = null)
        {
            if (expr == null) return null;
            var compiled = Compile(expr, table, parameters);
            return rec => compiled(rec);
        }

        private static Func<ParadoxRecord, bool> Compile(WhereExpr expr, ParadoxFile table,
            IDictionary<string, object> parameters)
        {
            switch (expr)
            {
                case WhereComparison cmp:
                {
                    int fieldIndex = ResolveFieldIndex(table, cmp.ColumnName);
                    var field = table.FieldTypes[fieldIndex];
                    var literal = SqlValueCoercion.Coerce(cmp.Value, field, parameters);
                    var op = cmp.Operator;
                    return rec =>
                    {
                        var actual = rec.DataValues[fieldIndex];
                        if (actual == null) return false; // NULL never matches a comparison
                        int comp = CompareValues(actual, literal);
                        switch (op)
                        {
                            case ParadoxCompareOperator.Equal: return comp == 0;
                            case ParadoxCompareOperator.NotEqual: return comp != 0;
                            case ParadoxCompareOperator.Greater: return comp > 0;
                            case ParadoxCompareOperator.GreaterOrEqual: return comp >= 0;
                            case ParadoxCompareOperator.Less: return comp < 0;
                            case ParadoxCompareOperator.LessOrEqual: return comp <= 0;
                            default: throw new SqlExecutionException($"Unsupported operator '{op}'.");
                        }
                    };
                }
                case WhereAnd and_:
                {
                    var left = Compile(and_.Left, table, parameters);
                    var right = Compile(and_.Right, table, parameters);
                    return rec => left(rec) && right(rec);
                }
                case WhereOr or_:
                {
                    var left = Compile(or_.Left, table, parameters);
                    var right = Compile(or_.Right, table, parameters);
                    return rec => left(rec) || right(rec);
                }
                default:
                    throw new SqlExecutionException($"Unsupported WHERE expression node '{expr.GetType().Name}'.");
            }
        }

        /// <summary>
        /// Same ordinal string comparison rule as <see cref="ParadoxCondition.Compare"/>
        /// uses, so full-scan filtering agrees with index-based filtering.
        /// </summary>
        private static int CompareValues(object a, object b)
        {
            if (a is string sa && b is string sb)
                return string.CompareOrdinal(sa, sb);
            return System.Collections.Comparer.Default.Compare(a, b);
        }

        public static int ResolveFieldIndex(ParadoxFile table, string columnName)
        {
            int idx = Array.FindIndex(table.FieldNames,
                f => string.Equals(f, columnName, StringComparison.OrdinalIgnoreCase));
            if (idx < 0)
                throw new SqlExecutionException($"Unknown column '{columnName}'.");
            return idx;
        }

        /// <summary>
        /// Attempts to build an index-accelerated <see cref="ParadoxCondition"/> for
        /// a single top-level equality comparison against the primary key's leading
        /// field. Returns null if the WHERE clause isn't a plain equality
        /// comparison eligible for primary-index traversal (falls back to full scan).
        /// </summary>
        public static ParadoxCondition TryBuildPrimaryIndexCondition(WhereExpr expr, ParadoxFile table,
            IDictionary<string, object> parameters = null)
        {
            if (!(expr is WhereComparison cmp)) return null;
            if (cmp.Operator != ParadoxCompareOperator.Equal &&
                cmp.Operator != ParadoxCompareOperator.Greater &&
                cmp.Operator != ParadoxCompareOperator.GreaterOrEqual &&
                cmp.Operator != ParadoxCompareOperator.Less &&
                cmp.Operator != ParadoxCompareOperator.LessOrEqual)
                return null;

            int fieldIndex = ResolveFieldIndex(table, cmp.ColumnName);
            if (fieldIndex != 0) return null; // primary key index only covers the leading field(s); only field 0 is safely usable here

            var field = table.FieldTypes[fieldIndex];
            var literal = SqlValueCoercion.Coerce(cmp.Value, field, parameters);
            return new ParadoxCondition.Compare(cmp.Operator, literal, fieldIndex, fieldIndex);
        }

        /// <summary>
        /// Attempts to build an index-accelerated <see cref="ParadoxCondition"/> for
        /// a single top-level comparison against a secondary index's leading key
        /// field. Returns null if not eligible.
        /// </summary>
        public static ParadoxCondition TryBuildSecondaryIndexCondition(WhereExpr expr, ParadoxFile table, SecondaryIndexHandle handle)
        {
            if (!(expr is WhereComparison cmp)) return null;

            int dataFieldIndex = ResolveFieldIndex(table, cmp.ColumnName);
            var fieldIndices = handle.FieldIndices;
            if (fieldIndices == null || fieldIndices.Length == 0) return null;
            if (fieldIndices[0] != dataFieldIndex) return null; // must match the index's leading key field

            var field = table.FieldTypes[dataFieldIndex];
            var literal = SqlValueCoercion.Coerce(cmp.Value, field);
            // For secondary indexes, IndexFieldIndex is the position within the
            // composed key (0, since it's the leading field), not dataFieldIndex.
            return new ParadoxCondition.Compare(cmp.Operator, literal, dataFieldIndex, 0);
        }
    }
}
