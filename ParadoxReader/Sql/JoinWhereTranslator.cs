using System;
using System.Collections.Generic;
using System.Linq;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Alias-aware counterpart to <see cref="WhereTranslator"/> for compiling a
    /// WHERE clause into a predicate over the multi-table row shape used by
    /// joined SELECTs (see <see cref="ParadoxSqlExecutor.ExecuteJoinedSelect"/>):
    /// one <see cref="ParadoxRecord"/> per table in FROM/JOIN order, indexed by
    /// table alias.
    /// </summary>
    internal static class JoinWhereTranslator
    {
        public static Func<ParadoxRecord[], bool> ToPredicate(WhereExpr expr,
            Dictionary<string, int> aliasToTableIndex, List<ParadoxFile> tables,
            IDictionary<string, object> parameters)
        {
            if (expr == null) return null;
            return Compile(expr, aliasToTableIndex, tables, parameters);
        }

        private static Func<ParadoxRecord[], bool> Compile(WhereExpr expr,
            Dictionary<string, int> aliasToTableIndex, List<ParadoxFile> tables,
            IDictionary<string, object> parameters)
        {
            switch (expr)
            {
                case WhereComparison cmp:
                {
                    int tableIndex = ResolveTableIndex(cmp.TableAlias, aliasToTableIndex, tables.Count);
                    var table = tables[tableIndex];
                    int fieldIndex = WhereTranslator.ResolveFieldIndex(table, cmp.ColumnName);
                    var field = table.FieldTypes[fieldIndex];
                    var literal = SqlValueCoercion.Coerce(cmp.Value, field, parameters);
                    var op = cmp.Operator;
                    return row =>
                    {
                        var rec = row[tableIndex];
                        if (rec == null) return false; // unmatched LEFT JOIN side never satisfies a WHERE comparison
                        var actual = rec.DataValues[fieldIndex];
                        if (actual == null) return false;
                        int comp = WhereTranslator.CompareValues(actual, literal);
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
                    var left = Compile(and_.Left, aliasToTableIndex, tables, parameters);
                    var right = Compile(and_.Right, aliasToTableIndex, tables, parameters);
                    return row => left(row) && right(row);
                }
                case WhereOr or_:
                {
                    var left = Compile(or_.Left, aliasToTableIndex, tables, parameters);
                    var right = Compile(or_.Right, aliasToTableIndex, tables, parameters);
                    return row => left(row) || right(row);
                }
                default:
                    throw new SqlExecutionException($"Unsupported WHERE expression node '{expr.GetType().Name}'.");
            }
        }

        private static int ResolveTableIndex(string alias, Dictionary<string, int> aliasToTableIndex, int tableCount)
        {
            if (alias == null)
            {
                if (tableCount != 1)
                    throw new SqlExecutionException("WHERE columns must be qualified with a table alias in a multi-table statement.");
                return 0;
            }

            if (!aliasToTableIndex.TryGetValue(alias, out int tableIndex))
                throw new SqlExecutionException($"Unknown table alias '{alias}'.");
            return tableIndex;
        }
    }
}
