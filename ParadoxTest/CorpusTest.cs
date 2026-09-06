using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// General-purpose, schema-agnostic regression test mode. Walks every
    /// table found under a data directory, infers each table's schema
    /// dynamically from its own header (field names/types, primary key,
    /// discovered secondary indexes), and exercises append / update / read /
    /// primary-lookup / secondary-lookup operations against a disposable
    /// working copy via ParadoxReader, then repeats an equivalent sequence
    /// via SQLRunner against a second disposable working copy of the same
    /// table, comparing structural results (record counts, presence of
    /// looked-up rows) between the two.
    ///
    /// The data directory to scan is resolved, in order: the "corpusRoot"
    /// argument; the "CorpusDataRootPath" appSetting (see
    /// SqlRunner.local.config.example); the bundled bin\Debug\data fixture
    /// folder (always present, so this mode works out of the box even with
    /// no machine-specific configuration).
    ///
    /// Some tables (e.g. real-world corpus data) may be password-protected;
    /// ParadoxReader does not currently implement Paradox's encryption, so
    /// those tables are expected to fail. Failures are caught and reported
    /// per-table without aborting the rest of the run.
    /// </summary>
    internal static class CorpusTest
    {
        private const string WorkRoot = @"c:\temp\corpustest";

        // Machine-specific; sourced from app.config's appSettings (via
        // SqlRunner.local.config, git-ignored) rather than hard-coded.
        private static string SqlRunnerExePath => Configuration.GetSqlRunnerExePath();

        private enum TableOutcome { Pass, Fail, Error, Skip }

        private class TableResult
        {
            public string TableBaseName;
            public TableOutcome Outcome;
            public string Detail;
        }

        // ----------------------------------------------------------------
        // Entry point
        // ----------------------------------------------------------------

        /// <summary>
        /// Runs the full corpus scan.
        /// </summary>
        /// <param name="corpusRoot">Directory containing the *.DB tables to scan.</param>
        /// <param name="maxTables">Optional cap on number of tables processed (0 = no limit); useful for smoke runs.</param>
        /// <param name="filter">Optional substring filter on table base name (case-insensitive).</param>
        public static void Run(string corpusRoot, int maxTables, string filter)
        {
            if (Net35Compat.IsNullOrWhiteSpace(corpusRoot))
                corpusRoot = Configuration.GetCorpusDataRootPath();

            // Final fallback: the bundled fixture data folder copied to the
            // output directory (bin\Debug\data) alongside the exe, so this
            // mode always has *something* to run against out of the box,
            // without requiring any machine-specific configuration.
            if (Net35Compat.IsNullOrWhiteSpace(corpusRoot))
            {
                corpusRoot = Path.Combine(
                    Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location), "data");
            }

            if (Net35Compat.IsNullOrWhiteSpace(corpusRoot) || !Directory.Exists(corpusRoot))
            {
                Console.WriteLine("[corpustest] Corpus root not found: {0}", corpusRoot);
                Console.WriteLine("[corpustest] Pass a corpusRoot argument, or set the \"CorpusDataRootPath\" " +
                    "appSetting in SqlRunner.local.config (see SqlRunner.local.config.example).");
                return;
            }

            bool haveSqlRunner = File.Exists(SqlRunnerExePath);
            if (!haveSqlRunner)
            {
                Console.WriteLine("[corpustest] [warn] SQLRunner not found at {0}; SQLRunner-side comparison will be skipped for every table.", SqlRunnerExePath);
            }

            Directory.CreateDirectory(WorkRoot);

            var tableBaseNames = Directory.GetFiles(corpusRoot, "*.DB", SearchOption.TopDirectoryOnly)
                .Select(f => Path.GetFileNameWithoutExtension(f))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (!Net35Compat.IsNullOrWhiteSpace(filter))
            {
                tableBaseNames = tableBaseNames
                    .Where(n => n.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0)
                    .ToList();
            }

            if (maxTables > 0 && tableBaseNames.Count > maxTables)
            {
                // Random sample rather than the first N alphabetically, so a
                // small default run (see Program.cs) still exercises a
                // representative cross-section of the corpus instead of
                // always the same alphabetically-first tables.
                var rng = new Random();
                tableBaseNames = tableBaseNames
                    .OrderBy(_ => rng.Next())
                    .Take(maxTables)
                    .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }

            Console.WriteLine("[corpustest] Corpus root: {0}", corpusRoot);
            Console.WriteLine("[corpustest] Tables to process: {0}", tableBaseNames.Count);

            var results = new List<TableResult>();
            var overallSw = Stopwatch.StartNew();

            for (int i = 0; i < tableBaseNames.Count; i++)
            {
                string baseName = tableBaseNames[i];
                Console.WriteLine();
                Console.WriteLine("=== [corpustest] ({0}/{1}) Table: {2} ===", i + 1, tableBaseNames.Count, baseName);

                var result = new TableResult { TableBaseName = baseName };
                try
                {
                    result.Outcome = ProcessTable(corpusRoot, baseName, haveSqlRunner, out string detail);
                    result.Detail = detail;
                }
                catch (Exception ex)
                {
                    result.Outcome = TableOutcome.Error;
                    result.Detail = ex.GetType().Name + ": " + ex.Message;
                    Console.WriteLine("  [error] Unhandled exception: {0}", result.Detail);
                }

                results.Add(result);
            }

            overallSw.Stop();

            Console.WriteLine();
            Console.WriteLine("=== [corpustest] SUMMARY ({0:0.0}s total) ===", overallSw.Elapsed.TotalSeconds);
            foreach (var group in results.GroupBy(r => r.Outcome).OrderBy(g => g.Key.ToString()))
            {
                Console.WriteLine("  {0}: {1}", group.Key, group.Count());
            }

            var failuresOrErrors = results.Where(r => r.Outcome == TableOutcome.Fail || r.Outcome == TableOutcome.Error).ToList();
            if (failuresOrErrors.Count > 0)
            {
                Console.WriteLine();
                Console.WriteLine("  Non-passing tables:");
                foreach (var r in failuresOrErrors)
                    Console.WriteLine("    [{0}] {1}: {2}", r.Outcome, r.TableBaseName, r.Detail);
            }
        }

        // ----------------------------------------------------------------
        // Per-table pipeline
        // ----------------------------------------------------------------

        private static TableOutcome ProcessTable(string corpusRoot, string baseName, bool haveSqlRunner, out string detail)
        {
            string harnessDir   = Path.Combine(Path.Combine(WorkRoot, SanitizeForPath(baseName)), "harness");
            string sqlrunnerDir = Path.Combine(Path.Combine(WorkRoot, SanitizeForPath(baseName)), "sqlrunner");

            try
            {
                StageWorkingCopy(corpusRoot, baseName, harnessDir);

                string harnessDbPath = Path.Combine(harnessDir, baseName + ".DB");
                if (!File.Exists(harnessDbPath))
                {
                    detail = "No .DB file staged (unexpected).";
                    return TableOutcome.Skip;
                }

                TableSchemaInfo schema;
                int baselineCount;
                var subResults = new List<string>();
                bool anySubFail = false;

                // Hoisted so the post-rebuild verification pass (below) can
                // re-check the same primary-key/secondary-index min/max ranges
                // against the rebuilt table without recomputing them.
                object[] pkMin = null, pkMax = null;
                var secondaryMinMax = new Dictionary<int, MinMax>();

                using (var table = new ParadoxTableFile(harnessDbPath))
                {
                    schema = CaptureSchema(table);
                    Console.WriteLine("  Fields ({0}): {1}", schema.FieldCount, string.Join(", ", schema.FieldNames));
                    Console.WriteLine("  PrimaryKeyFields={0} ({1}), SecondaryIndexes={2}",
                        schema.PrimaryKeyFieldCount,
                        table.PrimaryKeyIndex != null ? "present" : "absent",
                        table.SecondaryIndexes.Count);

                    // --- Baseline full scan ---
                    baselineCount = 0;

                    foreach (var rec in table.Enumerate())
                    {
                        baselineCount++;
                        if (schema.PrimaryKeyFieldCount > 0)
                            TrackMinMax(rec.DataValues, 0, ref pkMin, ref pkMax);

                        foreach (var idx in table.SecondaryIndexes)
                        {
                            if (idx.FieldIndices.Length == 0) continue;
                            int fi = idx.FieldIndices[0];
                            if (fi < 0 || fi >= rec.DataValues.Length) continue;
                            var val = rec.DataValues[fi];
                            if (!secondaryMinMax.TryGetValue(fi, out var mm))
                            {
                                secondaryMinMax[fi] = new MinMax { Min = val, Max = val };
                            }
                            else
                            {
                                object newMin = CompareForMinMax(mm.Min, val) <= 0 ? mm.Min : val;
                                object newMax = CompareForMinMax(mm.Max, val) >= 0 ? mm.Max : val;
                                secondaryMinMax[fi] = new MinMax { Min = newMin, Max = newMax };
                            }
                        }
                    }
                    Console.WriteLine("  [Test 1: Full scan] {0} record(s).", baselineCount);
                    subResults.Add("scan=PASS(" + baselineCount + ")");

                    // --- Append ---
                    try
                    {
                        var newValues = BuildSampleFieldValues(schema);
                        var appended = table.AppendRecord(newValues);
                        Console.WriteLine("  [Test 2: Append] OK - block={0} idx={1}", appended.BlockNumber, appended.RecordIndex);
                        subResults.Add("append=PASS");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("  [Test 2: Append] FAIL - {0}", ex.Message);
                        subResults.Add("append=FAIL:" + ex.GetType().Name);
                        anySubFail = true;
                    }

                    // --- Update ---
                    try
                    {
                        ParadoxRecord target = null;
                        foreach (var rec in table.Enumerate()) { target = rec; break; }

                        if (target != null)
                        {
                            var updatedValues = (object[])target.DataValues.Clone();
                            MutateOneMutableField(updatedValues, schema);
                            table.UpdateRecord(target, updatedValues);
                            Console.WriteLine("  [Test 3: Update] OK - updated first record.");
                            subResults.Add("update=PASS");
                        }
                        else
                        {
                            Console.WriteLine("  [Test 3: Update] SKIP - table has no records to update.");
                            subResults.Add("update=SKIP");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("  [Test 3: Update] FAIL - {0}", ex.Message);
                        subResults.Add("update=FAIL:" + ex.GetType().Name);
                        anySubFail = true;
                    }

                    // --- Primary key lookup ---
                    if (table.PrimaryKeyIndex != null && pkMin != null)
                    {
                        try
                        {
                            int expected = 0;
                            foreach (var rec in table.Enumerate())
                                if (CompareForMinMax(rec.DataValues[0], pkMin[0]) >= 0 && CompareForMinMax(rec.DataValues[0], pkMax[0]) <= 0)
                                    expected++;

                            var cond = new ParadoxCondition.LogicalAnd(
                                new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, pkMin[0], 0, 0),
                                new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, pkMax[0], 0, 0));

                            int viaIndex = 0;
                            using (var rdr = new ParadoxDataReader(table, table.PrimaryKeyIndex.Enumerate(cond)))
                                while (rdr.Read()) viaIndex++;

                            bool ok = viaIndex == expected;
                            Console.WriteLine("  [Test 4: PK lookup] expected={0} viaIndex={1} - {2}", expected, viaIndex, ok ? "PASS" : "FAIL");
                            subResults.Add("pklookup=" + (ok ? "PASS" : "FAIL"));
                            if (!ok) anySubFail = true;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("  [Test 4: PK lookup] FAIL - {0}", ex.Message);
                            subResults.Add("pklookup=FAIL:" + ex.GetType().Name);
                            anySubFail = true;
                        }
                    }
                    else
                    {
                        subResults.Add("pklookup=SKIP");
                    }

                    // --- Secondary index lookups ---
                    foreach (var idx in table.SecondaryIndexes)
                    {
                        if (idx.FieldIndices.Length == 0) continue;
                        int fi = idx.FieldIndices[0];
                        if (!secondaryMinMax.TryGetValue(fi, out var mm)) continue;

                        try
                        {
                            int expected = 0;
                            foreach (var rec in table.Enumerate())
                                if (fi < rec.DataValues.Length &&
                                    CompareForMinMax(rec.DataValues[fi], mm.Min) >= 0 &&
                                    CompareForMinMax(rec.DataValues[fi], mm.Max) <= 0)
                                    expected++;

                            var cond = new ParadoxCondition.LogicalAnd(
                                new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, mm.Min, fi, 0),
                                new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, mm.Max, fi, 0));

                            int viaIndex = 0;
                            using (var rdr = new ParadoxDataReader(table, idx.Enumerate(cond)))
                                while (rdr.Read()) viaIndex++;

                            bool ok = viaIndex == expected;
                            Console.WriteLine("  [Test 5: Secondary lookup field#{0}] expected={1} viaIndex={2} - {3}",
                                fi, expected, viaIndex, ok ? "PASS" : "FAIL");
                            subResults.Add("secidx" + fi + "=" + (ok ? "PASS" : "FAIL"));
                            if (!ok) anySubFail = true;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("  [Test 5: Secondary lookup field#{0}] FAIL - {1}", fi, ex.Message);
                            subResults.Add("secidx" + fi + "=FAIL:" + ex.GetType().Name);
                            anySubFail = true;
                        }
                    }
                }

                // --- SQLRunner comparison copy ---
                if (haveSqlRunner)
                {
                    try
                    {
                        StageWorkingCopy(corpusRoot, baseName, sqlrunnerDir);
                        string sqlOutcome = RunSqlRunnerComparison(sqlrunnerDir, baseName, schema, baselineCount);
                        Console.WriteLine("  [Test 6: SQLRunner comparison] {0}", sqlOutcome);
                        subResults.Add("sqlrunner=" + sqlOutcome);
                        if (sqlOutcome.StartsWith("FAIL")) anySubFail = true;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("  [Test 6: SQLRunner comparison] FAIL - {0}", ex.Message);
                        subResults.Add("sqlrunner=FAIL:" + ex.GetType().Name);
                        anySubFail = true;
                    }
                }
                else
                {
                    subResults.Add("sqlrunner=SKIP");
                }

                // --- Rebuild (compact and repair) + re-verify ---
                try
                {
                    string rebuildOutcome = RunRebuildAndVerify(harnessDbPath, schema, pkMin, pkMax, secondaryMinMax);
                    Console.WriteLine("  [Test 7: Rebuild + reread] {0}", rebuildOutcome);
                    subResults.Add("rebuild=" + rebuildOutcome);
                    if (rebuildOutcome.StartsWith("FAIL")) anySubFail = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("  [Test 7: Rebuild + reread] FAIL - {0}", ex.Message);
                    subResults.Add("rebuild=FAIL:" + ex.GetType().Name);
                    anySubFail = true;
                }

                detail = string.Join(", ", subResults.ToArray());
                return anySubFail ? TableOutcome.Fail : TableOutcome.Pass;
            }
            finally
            {
                CleanupDirectory(harnessDir);
                CleanupDirectory(sqlrunnerDir);
            }
        }

        // ----------------------------------------------------------------
        // Schema capture
        // ----------------------------------------------------------------

        private class TableSchemaInfo
        {
            public string[] FieldNames;
            public ParadoxFile.FieldInfo[] FieldTypes;
            public int FieldCount;
            public int PrimaryKeyFieldCount;
        }

        private class MinMax
        {
            public object Min;
            public object Max;
        }

        private static TableSchemaInfo CaptureSchema(ParadoxTableFile table)
        {
            return new TableSchemaInfo
            {
                FieldNames            = table.FieldNames,
                FieldTypes            = table.FieldTypes,
                FieldCount            = table.FieldCount,
                PrimaryKeyFieldCount  = table.PrimaryKeyIndex != null ? CountPrimaryKeyFields(table) : 0
            };
        }

        // primaryKeyFields is an internal field on ParadoxFile; ParadoxTest has
        // InternalsVisibleTo access to it via the field-info array length used
        // by ParadoxPrimaryKey itself. We infer the count the same way
        // ParadoxPrimaryKey does (first N field types used as the key).
        private static int CountPrimaryKeyFields(ParadoxTableFile table)
        {
            // ParadoxPrimaryKey doesn't expose the raw count directly, but its
            // FieldIndices-equivalent isn't public either; conservatively
            // treat the first field as the (at least single) primary key
            // field, which is true for every AUTOINC/simple-PK table we've
            // seen in this corpus. Composite primary keys still work for
            // lookups since ParadoxCondition.Compare only needs the first
            // key field's position (0) for range pruning purposes here.
            return 1;
        }

        // ----------------------------------------------------------------
        // Sample value / mutation generation (dynamic, schema-driven)
        // ----------------------------------------------------------------

        private static object[] BuildSampleFieldValues(TableSchemaInfo schema)
        {
            var values = new object[schema.FieldCount];
            for (int i = 0; i < schema.FieldCount; i++)
                values[i] = GenerateSampleValue(schema.FieldTypes[i]);
            return values;
        }

        private static object GenerateSampleValue(ParadoxFile.FieldInfo field)
        {
            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                    return "corpustest";
                case ParadoxFieldTypes.Short:
                    return (short)1;
                case ParadoxFieldTypes.Long:
                    return 1;
                case ParadoxFieldTypes.AutoInc:
                    return null; // auto-assigned by AppendRecord
                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    return 1.0;
                case ParadoxFieldTypes.Date:
                    return DateTime.Today;
                case ParadoxFieldTypes.Time:
                    return new TimeSpan(1, 2, 3);
                case ParadoxFieldTypes.Timestamp:
                    return DateTime.Now;
                case ParadoxFieldTypes.Logical:
                    return false;
                case ParadoxFieldTypes.BCD:
                    return 1.0m;
                case ParadoxFieldTypes.Bytes:
                    return new byte[field.fSize];
                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                    return null; // leave memo empty rather than fabricate blob refs
                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                    return null;
                default:
                    return null;
            }
        }

        /// <summary>
        /// Mutates the first Alpha/Number/Short/Long field found (skipping the
        /// primary key at index 0) so the update test doesn't require any
        /// hardcoded field name knowledge.
        /// </summary>
        private static void MutateOneMutableField(object[] values, TableSchemaInfo schema)
        {
            for (int i = 1; i < schema.FieldCount && i < values.Length; i++)
            {
                switch (schema.FieldTypes[i].fType)
                {
                    case ParadoxFieldTypes.Alpha:
                        values[i] = "corpustest-upd";
                        return;
                    case ParadoxFieldTypes.Short:
                        values[i] = (short)((values[i] is short s ? s : (short)0) + 1);
                        return;
                    case ParadoxFieldTypes.Long:
                        values[i] = (values[i] is int n ? n : 0) + 1;
                        return;
                    case ParadoxFieldTypes.Number:
                    case ParadoxFieldTypes.Currency:
                        values[i] = (values[i] is double d ? d : 0.0) + 1.0;
                        return;
                }
            }
            // No safely mutable field found; leave values unchanged (update
            // still exercises the write path even if it's a no-op change).
        }

        private static void TrackMinMax(object[] values, int fieldIndex, ref object[] min, ref object[] max)
        {
            if (fieldIndex >= values.Length) return;
            if (min == null)
            {
                min = new object[] { values[fieldIndex] };
                max = new object[] { values[fieldIndex] };
                return;
            }
            if (CompareForMinMax(values[fieldIndex], min[0]) < 0) min[0] = values[fieldIndex];
            if (CompareForMinMax(values[fieldIndex], max[0]) > 0) max[0] = values[fieldIndex];
        }

        private static int CompareForMinMax(object a, object b)
        {
            if (a == null || b == null) return 0;
            if (a is string sa && b is string sb) return string.CompareOrdinal(sa, sb);
            try { return System.Collections.Comparer.Default.Compare(a, b); }
            catch { return 0; }
        }

        // ----------------------------------------------------------------
        // Working-copy staging
        // ----------------------------------------------------------------

        private static void StageWorkingCopy(string corpusRoot, string baseName, string destDir)
        {
            CleanupDirectory(destDir);
            Directory.CreateDirectory(destDir);

            foreach (var sourceFile in Directory.GetFiles(corpusRoot, baseName + ".*"))
            {
                var destFile = Path.Combine(destDir, Path.GetFileName(sourceFile));
                File.Copy(sourceFile, destFile, overwrite: true);
            }
        }

        private static void CleanupDirectory(string dir)
        {
            if (string.IsNullOrEmpty(dir) || !Directory.Exists(dir)) return;
            try { Directory.Delete(dir, recursive: true); }
            catch { /* best effort - a stray lock/handle can prevent immediate cleanup */ }
        }

        private static string SanitizeForPath(string name)
        {
            foreach (var c in Path.GetInvalidFileNameChars())
                name = name.Replace(c, '_');
            return name;
        }

        // ----------------------------------------------------------------
        // Rebuild (compact and repair) verification
        // ----------------------------------------------------------------

        /// <summary>
        /// Rebuilds <paramref name="dbPath"/> via <see cref="TableRebuilder.Rebuild(string, string)"/>
        /// and re-reads the result, checking that the record count and the
        /// same primary-key/secondary-index range lookups computed against
        /// the original table still hold against the rebuilt one. This
        /// confirms TableRebuilder's output is itself readable (both by a
        /// full scan and via its freshly rebuilt indexes) - i.e. that the
        /// original table and the rebuilt table both work.
        /// </summary>
        private static string RunRebuildAndVerify(
            string dbPath, TableSchemaInfo schema,
            object[] pkMin, object[] pkMax, Dictionary<int, MinMax> secondaryMinMax)
        {
            // Capture the current record count immediately before rebuilding
            // (rather than the original pre-append baseline), since by this
            // point in the pipeline the harness table has already had a
            // record appended/updated by earlier test steps.
            int expectedCount;
            using (var preTable = new ParadoxTableFile(dbPath))
            {
                expectedCount = 0;
                foreach (var _ in preTable.Enumerate()) expectedCount++;
            }

            var result = TableRebuilder.Rebuild(dbPath);

            using (var table = new ParadoxTableFile(dbPath))
            {
                int postCount = 0;
                foreach (var _ in table.Enumerate()) postCount++;

                if (postCount != expectedCount)
                    return "FAIL(count-mismatch:pre=" + expectedCount + ",post=" + postCount + ")";

                // --- Primary key lookup against rebuilt indexes ---
                if (table.PrimaryKeyIndex != null && pkMin != null)
                {
                    int expected = 0;
                    foreach (var rec in table.Enumerate())
                        if (CompareForMinMax(rec.DataValues[0], pkMin[0]) >= 0 && CompareForMinMax(rec.DataValues[0], pkMax[0]) <= 0)
                            expected++;

                    var cond = new ParadoxCondition.LogicalAnd(
                        new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, pkMin[0], 0, 0),
                        new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, pkMax[0], 0, 0));

                    int viaIndex = 0;
                    using (var rdr = new ParadoxDataReader(table, table.PrimaryKeyIndex.Enumerate(cond)))
                        while (rdr.Read()) viaIndex++;

                    if (viaIndex != expected)
                        return "FAIL(pklookup:expected=" + expected + ",viaIndex=" + viaIndex + ")";
                }

                // --- Secondary index lookups against rebuilt indexes ---
                foreach (var idx in table.SecondaryIndexes)
                {
                    if (idx.FieldIndices.Length == 0) continue;
                    int fi = idx.FieldIndices[0];
                    if (!secondaryMinMax.TryGetValue(fi, out var mm)) continue;

                    int expected = 0;
                    foreach (var rec in table.Enumerate())
                        if (fi < rec.DataValues.Length &&
                            CompareForMinMax(rec.DataValues[fi], mm.Min) >= 0 &&
                            CompareForMinMax(rec.DataValues[fi], mm.Max) <= 0)
                            expected++;

                    var cond = new ParadoxCondition.LogicalAnd(
                        new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, mm.Min, fi, 0),
                        new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, mm.Max, fi, 0));

                    int viaIndex = 0;
                    using (var rdr = new ParadoxDataReader(table, idx.Enumerate(cond)))
                        while (rdr.Read()) viaIndex++;

                    if (viaIndex != expected)
                        return "FAIL(secidx" + fi + ":expected=" + expected + ",viaIndex=" + viaIndex + ")";
                }
            }

            return "PASS(migrated=" + result.RecordsMigrated + ")";
        }

        // ----------------------------------------------------------------
        // SQLRunner comparison
        // ----------------------------------------------------------------

        private static string RunSqlRunnerComparison(string workDir, string baseName, TableSchemaInfo schema, int preAppendCount)
        {
            string tablePath = Path.Combine(workDir, baseName + ".DB");

            // Build a generic INSERT skipping AutoInc fields (BDE auto-no-ops
            // an explicit AUTOINC value on INSERT, same as documented in
            // BuildInsertSql for the TESTTAB harness).
            var columns = new List<string>();
            var literals = new List<string>();
            for (int i = 0; i < schema.FieldCount; i++)
            {
                if (schema.FieldTypes[i].fType == ParadoxFieldTypes.AutoInc) continue;
                if (schema.FieldTypes[i].fType == ParadoxFieldTypes.MemoBLOb ||
                    schema.FieldTypes[i].fType == ParadoxFieldTypes.FmtMemoBLOb ||
                    schema.FieldTypes[i].fType == ParadoxFieldTypes.BLOb ||
                    schema.FieldTypes[i].fType == ParadoxFieldTypes.OLE ||
                    schema.FieldTypes[i].fType == ParadoxFieldTypes.Graphic ||
                    schema.FieldTypes[i].fType == ParadoxFieldTypes.Bytes)
                    continue; // SQLRunner literal generation not attempted for blob/byte types

                string literal = GenerateSqlLiteral(schema.FieldTypes[i]);
                if (literal == null) continue;

                columns.Add("'" + schema.FieldNames[i] + "'");
                literals.Add(literal);
            }

            if (columns.Count == 0)
                return "SKIP:no-insertable-columns";

            string insertSql = "INSERT INTO '" + tablePath + "' (" + string.Join(", ", columns.ToArray()) + ") VALUES (" + string.Join(", ", literals.ToArray()) + ")";

            RunSqlRunner(workDir, insertSql);

            int postCount;
            using (var table = new ParadoxTableFile(tablePath))
            {
                postCount = 0;
                foreach (var _ in table.Enumerate()) postCount++;
            }

            bool ok = postCount == preAppendCount + 1;
            return (ok ? "PASS" : "FAIL") + "(pre=" + preAppendCount + ",post=" + postCount + ")";
        }

        private static string GenerateSqlLiteral(ParadoxFile.FieldInfo field)
        {
            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                    return "'corpustest'";
                case ParadoxFieldTypes.Short:
                    return "1";
                case ParadoxFieldTypes.Long:
                    return "1";
                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    return "1";
                case ParadoxFieldTypes.Date:
                    return "'" + DateTime.Today.ToString("MM/dd/yyyy") + "'";
                case ParadoxFieldTypes.Time:
                    return "'01:02:03'";
                case ParadoxFieldTypes.Timestamp:
                    return "'" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "'";
                case ParadoxFieldTypes.Logical:
                    return "FALSE";
                case ParadoxFieldTypes.BCD:
                    return "1";
                default:
                    return null;
            }
        }

        /// <summary>
        /// Minimal, self-contained SQLRunner invocation for the corpus test
        /// mode (deliberately separate from Program.RunSqlRunner, which is
        /// scoped to the single-table TESTTAB harness/TestFolder constant).
        /// Applies the same hang-detection/timeout tolerance since SQLRunner
        /// can wait on stdin for UPDATE/DELETE confirmation prompts.
        /// </summary>
        private static void RunSqlRunner(string workDir, string sql)
        {
            var psi = new ProcessStartInfo
            {
                FileName               = SqlRunnerExePath,
                Arguments              = $"/S \"{sql}\"",
                UseShellExecute        = false,
                RedirectStandardInput  = true,
                RedirectStandardOutput = true,
                RedirectStandardError  = true,
                CreateNoWindow         = true
            };

            using (var process = new Process { StartInfo = psi })
            {
                process.Start();

                try
                {
                    process.StandardInput.WriteLine();
                    process.StandardInput.Flush();
                }
                catch { /* process may have already exited */ }

                if (!process.WaitForExit(10000))
                {
                    try
                    {
                        using (var killer = new Process())
                        {
                            killer.StartInfo = new ProcessStartInfo
                            {
                                FileName        = "taskkill",
                                Arguments       = $"/PID {process.Id} /T /F",
                                UseShellExecute = false,
                                CreateNoWindow  = true,
                                RedirectStandardOutput = true,
                                RedirectStandardError  = true
                            };
                            killer.Start();
                            killer.WaitForExit(5000);
                        }
                    }
                    catch { /* best effort */ }
                    try { if (!process.HasExited) process.Kill(); } catch { /* best effort */ }
                    process.WaitForExit();
                }
            }

            System.Threading.Thread.Sleep(300);

            foreach (var lockFile in Directory.GetFiles(workDir, "*.LCK"))
            {
                try { File.Delete(lockFile); } catch { /* best effort */ }
            }
        }
    }
}
