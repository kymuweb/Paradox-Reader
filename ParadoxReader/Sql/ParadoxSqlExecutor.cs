using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Executes parsed <see cref="SqlStatement"/> trees against a
    /// <see cref="ParadoxTableFile"/>. Scope is intentionally limited to
    /// single-table SELECT/INSERT/UPDATE/DELETE (see <see cref="SqlParser"/>
    /// grammar remarks) - no joins, aggregates, GROUP BY, or ORDER BY.
    ///
    /// Table paths in FROM/INTO/UPDATE/DELETE FROM are resolved relative to
    /// an optional <see cref="BaseDirectory"/> (bare table names, e.g.
    /// "testtab" or "testtab.db") or used as-is when they already look like
    /// a full/relative path.
    /// </summary>
    internal sealed class ParadoxSqlExecutor : IDisposable
    {
        /// <summary>
        /// Base directory used to resolve bare table names (no directory
        /// separator) that aren't already a full path. May be null, in which
        /// case bare names are resolved relative to the current directory.
        /// </summary>
        public string BaseDirectory { get; set; }

        // Tables opened during this executor's lifetime, keyed by resolved
        // full path, so repeated statements against the same table within one
        // "connection" reuse the same open ParadoxTableFile (and its index
        // state) rather than re-opening on every statement.
        private readonly Dictionary<string, ParadoxTableFile> openTables =
            new Dictionary<string, ParadoxTableFile>(StringComparer.OrdinalIgnoreCase);

        public ParadoxSqlExecutor(string baseDirectory = null)
        {
            BaseDirectory = baseDirectory;
        }

        public void Dispose()
        {
            foreach (var t in openTables.Values)
                t.Dispose();
            openTables.Clear();
        }

        /// <summary>
        /// Parses and executes a single SQL statement. For SELECT, returns an
        /// open <see cref="IDataReader"/> the caller must dispose/enumerate.
        /// For INSERT/UPDATE/DELETE, executes immediately and returns the
        /// number of affected rows via <paramref name="rowsAffected"/>,
        /// returning null.
        /// </summary>
        /// <param name="sql">SQL text, optionally containing "@name"/"?" parameter placeholders.</param>
        /// <param name="rowsAffected">Number of rows affected by INSERT/UPDATE/DELETE, or -1 for SELECT.</param>
        /// <param name="parameters">
        /// Bound parameter values keyed by name (case-insensitive, without a
        /// leading '@'/':'), or by synthetic "?N" keys for positional
        /// placeholders in declaration order. May be null if the statement has
        /// no placeholders.
        /// </param>
        public IDataReader Execute(string sql, out int rowsAffected, IDictionary<string, object> parameters = null)
        {
            var stmt = new SqlParser(sql).Parse();
            switch (stmt)
            {
                case SelectStatement select:
                    rowsAffected = -1;
                    return ExecuteSelect(select, parameters);

                case InsertStatement insert:
                    rowsAffected = ExecuteInsert(insert, parameters);
                    return null;

                case UpdateStatement update:
                    rowsAffected = ExecuteUpdate(update, parameters);
                    return null;

                case DeleteStatement delete:
                    rowsAffected = ExecuteDelete(delete, parameters);
                    return null;

                default:
                    throw new SqlExecutionException($"Unsupported statement type '{stmt.GetType().Name}'.");
            }
        }

        /// <summary>Convenience overload for INSERT/UPDATE/DELETE statements.</summary>
        public int ExecuteNonQuery(string sql, IDictionary<string, object> parameters = null)
        {
            Execute(sql, out int rowsAffected, parameters);
            return rowsAffected;
        }

        /// <summary>Convenience overload for SELECT statements.</summary>
        public IDataReader ExecuteReader(string sql, IDictionary<string, object> parameters = null) =>
            Execute(sql, out _, parameters);

        // ----------------------------------------------------------------
        // Table resolution
        // ----------------------------------------------------------------

        private ParadoxTableFile ResolveTable(TableRef tableRef)
        {
            string resolvedPath = ResolvePath(tableRef.Path);

            if (openTables.TryGetValue(resolvedPath, out var existing))
                return existing;

            var table = new ParadoxTableFile(resolvedPath);
            openTables[resolvedPath] = table;
            return table;
        }

        private string ResolvePath(string rawPath)
        {
            string path = rawPath;
            if (!path.EndsWith(".db", StringComparison.OrdinalIgnoreCase) &&
                !Path.HasExtension(path))
            {
                path += ".db";
            }

            if (!Path.IsPathRooted(path) && !path.Contains(Path.DirectorySeparatorChar) &&
                !path.Contains(Path.AltDirectorySeparatorChar) && BaseDirectory != null)
            {
                path = Path.Combine(BaseDirectory, path);
            }

            return Path.GetFullPath(path);
        }

        // ----------------------------------------------------------------
        // SELECT
        // ----------------------------------------------------------------

        private IDataReader ExecuteSelect(SelectStatement stmt, IDictionary<string, object> parameters)
        {
            var table = ResolveTable(stmt.Table);

            int[] columnIndices = stmt.IsSelectStar
                ? Enumerable.Range(0, table.FieldTypes.Length).ToArray()
                : stmt.Columns.Select(c => WhereTranslator.ResolveFieldIndex(table, c)).ToArray();

            IEnumerable<ParadoxRecord> rows = ExecuteWhereRead(table, stmt.Where, parameters);

            return new SqlDataReader(table, rows, columnIndices);
        }

        /// <summary>
        /// Chooses between index-accelerated and full-scan enumeration for a
        /// read-only (SELECT) statement's WHERE clause. Falls back to a
        /// full-table scan whenever no eligible index is found.
        /// </summary>
        private IEnumerable<ParadoxRecord> ExecuteWhereRead(ParadoxTableFile table, WhereExpr where,
            IDictionary<string, object> parameters)
        {
            if (where == null)
                return table.Enumerate();

            if (!table.IndexOutOfDate)
            {
                var pkCondition = WhereTranslator.TryBuildPrimaryIndexCondition(where, table, parameters);
                if (pkCondition != null && table.PrimaryKeyIndex != null && !table.PrimaryKeyIndex.IsOutOfDate)
                    return table.PrimaryKeyIndex.Enumerate(pkCondition);

                // Secondary-index-accelerated SELECT is intentionally not used
                // here: SecondaryIndexFile.Enumerate has known reliability gaps
                // (see SecondaryIndexFile remarks on pxLevelCount/block-range
                // leaf detection) that can silently under-return matching rows.
                // Full-table-scan filtering below is always correct even though
                // it's O(n), so it's the safe default until that read path is
                // hardened separately.
            }

            var predicate = WhereTranslator.ToPredicate(where, table, parameters);
            return table.Enumerate(predicate);
        }

        // ----------------------------------------------------------------
        // INSERT
        // ----------------------------------------------------------------

        private int ExecuteInsert(InsertStatement stmt, IDictionary<string, object> parameters)
        {
            var table = ResolveTable(stmt.Table);
            var fieldTypes = table.FieldTypes;
            var fieldValues = new object[fieldTypes.Length];

            List<int> targetIndices;
            if (stmt.Columns != null)
            {
                targetIndices = stmt.Columns.Select(c => WhereTranslator.ResolveFieldIndex(table, c)).ToList();
                if (targetIndices.Count != stmt.Values.Count)
                    throw new SqlExecutionException("Column list and VALUES list have different lengths.");
            }
            else
            {
                targetIndices = Enumerable.Range(0, fieldTypes.Length).ToList();
                if (targetIndices.Count != stmt.Values.Count)
                    throw new SqlExecutionException(
                        $"VALUES list has {stmt.Values.Count} value(s) but table has {fieldTypes.Length} field(s); " +
                        "specify an explicit column list to insert a subset of fields.");
            }

            for (int i = 0; i < targetIndices.Count; i++)
            {
                int fieldIndex = targetIndices[i];
                fieldValues[fieldIndex] = SqlValueCoercion.Coerce(stmt.Values[i], fieldTypes[fieldIndex], parameters);
            }

            // AutoInc fields are always assigned by AppendRecord itself, matching
            // BDE's own behavior of silently ignoring any user-supplied AUTOINC value
            // (see BuildInsertSql remarks in ParadoxTest/MiscTests.cs).
            table.AppendRecord(fieldValues);
            return 1;
        }

        // ----------------------------------------------------------------
        // UPDATE
        // ----------------------------------------------------------------

        private int ExecuteUpdate(UpdateStatement stmt, IDictionary<string, object> parameters)
        {
            var table = ResolveTable(stmt.Table);

            // Writes must never use a stale index for matching (ThrowIfOutOfDate
            // inside IndexManager/PrimaryIndexFile/SecondaryIndexFile guards actual
            // index writes performed as part of UpdateRecord, but the matching scan
            // here should also avoid index reads before that write is attempted).
            var predicate = WhereTranslator.ToPredicate(stmt.Where, table, parameters);
            var matches = table.Enumerate(predicate).ToList(); // materialize before mutating

            int count = 0;
            foreach (var rec in matches)
            {
                var newValues = rec.CloneDataValues();
                foreach (var assignment in stmt.Assignments)
                {
                    int fieldIndex = WhereTranslator.ResolveFieldIndex(table, assignment.ColumnName);
                    var existing = newValues[fieldIndex];
                    newValues[fieldIndex] = SqlValueCoercion.Coerce(assignment.Value, table.FieldTypes[fieldIndex], parameters, existing);
                }
                table.UpdateRecord(rec, newValues);
                count++;
            }
            return count;
        }

        // ----------------------------------------------------------------
        // DELETE
        // ----------------------------------------------------------------

        private int ExecuteDelete(DeleteStatement stmt, IDictionary<string, object> parameters)
        {
            var table = ResolveTable(stmt.Table);

            var predicate = WhereTranslator.ToPredicate(stmt.Where, table, parameters);
            var matches = table.Enumerate(predicate).ToList(); // materialize before mutating

            // Delete from the end backwards: ParadoxTableFile.DeleteRecord shifts
            // subsequent records within the same block down by one slot, which
            // would invalidate later (already-collected) RecordIndex values within
            // the same block if we deleted front-to-back instead.
            var ordered = matches
                .OrderByDescending(r => r.BlockNumber)
                .ThenByDescending(r => r.RecordIndex)
                .ToList();

            int count = 0;
            foreach (var rec in ordered)
            {
                table.DeleteRecord(rec);
                count++;
            }
            return count;
        }
    }
}
