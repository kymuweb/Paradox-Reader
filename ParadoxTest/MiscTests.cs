using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Test harness for ParadoxReader's write path (InsertRecord / AppendRecord /
    /// UpdateRecord / DeleteRecord) and index maintenance (.PX / .Xnn), validated
    /// against a SQLRunner + BDE's own Pdxrbld-style "no errors found"
    /// consistency checks.
    ///
    /// ------------------------------------------------------------------------
    /// TESTTAB.DB was created with the following BDE Local SQL, executed once via
    /// SQLRunner (kept here for reference/reproducibility):
    ///
    ///     CREATE TABLE "c:\temp\paradoxtest\testtab.db" (
    ///         ID           AUTOINC,
    ///         SECVAL       SMALLINT,
    ///         INTVAL       INTEGER,
    ///         DECVAL       DECIMAL(10, 2),
    ///         NUMVAL       NUMERIC(10, 2),
    ///         FLOATVAL     FLOAT(10, 2),
    ///         CHARVAL      CHARACTER(20),
    ///         VARCHARVAL   VARCHAR(50),
    ///         DATEVAL      DATE,
    ///         BOOLVAL      BOOLEAN,
    ///         MEMOVAL      BLOB(240, 1),
    ///         TIMEVAL      TIME,
    ///         TIMESTAMPVAL TIMESTAMP,
    ///         MONEYVAL     MONEY,
    ///         BYTESVAL     BYTES(10),
    ///         PRIMARY KEY (ID)
    ///     )
    ///
    ///     CREATE INDEX SECIDX ON "c:\temp\paradoxtest\testtab.db" (SECVAL)
    ///     CREATE INDEX IDXCHAR ON "c:\temp\paradoxtest\testtab.db" (CHARVAL)
    ///     CREATE INDEX IDXCOMPOSITE ON "c:\temp\paradoxtest\testtab.db" (INTVAL, DATEVAL)
    ///
    /// The resulting TESTTAB.DB/.PX/.MB and secondary index files (.XG0/.YG0
    /// for SECIDX, .XG1/.YG1 for IDXCHAR, .XG2/.YG2 for IDXCOMPOSITE) were
    /// then copied to the proj data folder and then copied to bin\Debug\data on
    /// every build (CopyToOutputDirectory=Always in ParadoxTest.csproj).
    /// ------------------------------------------------------------------------
    /// </summary>
    internal static class MiscTests
    {
        // 0-based field indexes, matching the CREATE TABLE column order above.
        private const int F_ID           = 0; // AUTOINC   (primary key)
        private const int F_SECVAL       = 1; // SMALLINT  (secondary key)
        private const int F_INTVAL       = 2; // INTEGER
        private const int F_DECVAL       = 3; // DECIMAL(10,2)
        private const int F_NUMVAL       = 4; // NUMERIC(10,2)
        private const int F_FLOATVAL     = 5; // FLOAT(10,2)
        private const int F_CHARVAL      = 6; // CHARACTER(20)
        private const int F_VARCHARVAL   = 7; // VARCHAR(50)
        private const int F_DATEVAL      = 8; // DATE
        private const int F_BOOLVAL      = 9; // BOOLEAN
        private const int F_MEMOVAL      = 10; // BLOB(240,1) memo
        private const int F_TIMEVAL      = 11; // TIME
        private const int F_TIMESTAMPVAL = 12; // TIMESTAMP
        private const int F_MONEYVAL     = 13; // MONEY
        private const int F_BYTESVAL     = 14; // BYTES(10)

        // Machine-specific; sourced from app.config's appSettings (via
        // SqlRunner.local.config, git-ignored) rather than hard-coded, so
        // it's never committed. SQLRunner is used to independently verify
        // (via SELECT) that writes performed by ParadoxReader are visible
        // to BDE, and to run Pdxrbld-equivalent consistency checks.
        private static string SqlRunnerExePath => Configuration.GetSqlRunnerExePath();

        // Paradox/BDE has historical issues with long paths and permissions on
        // some folders (Program Files, deeply nested repo paths, etc.), so all
        // test tables are copied to/run from c:\temp, which is short and always
        // writable.
        private const string TestFolder   = @"c:\temp\paradoxtest";
        // Full variant: .DB + .MB + .PX (primary index) + secondary indexes
        // SECIDX (.XG0/.YG0), IDXCHAR (.XG1/.YG1), IDXCOMPOSITE (.XG2/.YG2).
        // Both no-indices and no-secondary-index variants passed cleanly
        // after the block-chain fix; this is the final, most complete
        // variant to validate.
        //
        // Made non-const (was: private const string TableName = "TESTTAB.DB")
        // so the standard test suite can be re-run against other empty
        // fixture tables (e.g. TESTTAB_PASSWORDED.DB) via the "table"
        // command-line mode, without duplicating ~1000 lines of test code.
        private static string TestTabTableName = "TESTTAB.DB";

        private static string SourceDataFolder =>
            Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "data");

        // --------------------------------------------------------------------
        // Public entry points, dispatched from Program.Main.
        // --------------------------------------------------------------------

        public static void RunLibUpdateTest() => LibUpdateTest.Run();

        public static void RunLibCreateTest() => LibUpdateTest.RunCreateTest();

        public static void RunPxPasswordTestMode() => RunPxPasswordTest();

        public static void RunPxPasswordWriteTestMode(string dbPath) => RunPxPasswordWriteTest(dbPath);

        public static void RunSqlRunnerMode() => RunSqlRunnerOnlyMode();

        public static void RunHarnessMode() => RunHarnessOnlyMode();

        public static void RunCompareStepsMode() => CompareStepSnapshots();

        public static void RunGrowPxIndexModePublic(int targetCount) => RunGrowPxIndexMode(targetCount);

        public static void RunSuiteMode(string tableName)
        {
            TestTabTableName = !string.IsNullOrEmpty(tableName) ? tableName : TestTabTableName;
            RunStandardTestSuite();
        }

        public static void RunRebuildTestMode() => RunRebuildTest();

        public static void RunIndexOutOfDateTestMode() => RunIndexOutOfDateTest();

        public static void EnsureCleanState()
        {
            Directory.CreateDirectory(TestFolder);
            EnsureNoSqlRunnerProcessesRunning();
            DeleteStaleLockFilesWithRetry();
        }

        private static void RunStandardTestSuite()
        {
            ResetTestFolder();

            Console.WriteLine("=== Test 1: Append records ===");
            TestAppendRecords();

            Console.WriteLine();
            Console.WriteLine("=== Test 2: Update a record ===");
            TestUpdateRecord();

            Console.WriteLine();
            Console.WriteLine("=== Test 3: Insert into an explicit slot ===");
            TestInsertRecord();

            Console.WriteLine();
            Console.WriteLine("=== Test 4: Delete a record ===");
            TestDeleteRecord();

            Console.WriteLine();
            Console.WriteLine("=== Test 5: Read back all records ===");
            TestReadAllRecords();

            Console.WriteLine();
            Console.WriteLine("=== Test 5b: Condition + primary index lookup ===");
            TestConditionIndexLookup();

            Console.WriteLine();
            Console.WriteLine("=== Test 5c: Condition + secondary index lookup (SECIDX / SECVAL) ===");
            TestConditionSecondaryIndexLookup();

            Console.WriteLine();
            Console.WriteLine("=== Test 5d: Condition + secondary index lookup (IDXCHAR / CHARVAL) ===");
            TestConditionSecondaryIndexLookupCharVal();

            Console.WriteLine();
            Console.WriteLine("=== Test 5e: Condition + secondary index lookup (IDXCOMPOSITE / INTVAL, DATEVAL) ===");
            TestConditionSecondaryIndexLookupComposite();

            Console.WriteLine();
            Console.WriteLine("=== Test 6: Verify with SQLRunner ===");
            TestVerifyWithSqlRunner();

            Console.WriteLine();
            Console.WriteLine("All tests completed.");
        }

        // --------------------------------------------------------------------
        // Rebuild (compact & repair) test
        // --------------------------------------------------------------------

        /// <summary>
        /// Exercises <see cref="TableRebuilder.Rebuild(string, string)"/> end to
        /// end: stages the TESTTAB.DB fixture (with data appended so it has a
        /// non-trivial record set, memo values, and populated secondary
        /// indexes), captures a snapshot of every record/field value plus the
        /// record count, rebuilds the table from scratch, then re-opens the
        /// rebuilt table and verifies the record count and every field value
        /// match the pre-rebuild snapshot, and that both a primary-index and a
        /// secondary-index lookup still work correctly against the rebuilt
        /// indexes.
        /// </summary>
        private static void RunRebuildTest()
        {
            ResetTestFolder();

            Console.WriteLine("=== Rebuild test: seed data ===");
            TestAppendRecords();

            string dbPath = Path.Combine(TestFolder, TestTabTableName);

            // Snapshot every record's field values (as strings, so comparison
            // doesn't need to special-case MemoValue/byte[]/etc.) before the
            // rebuild, keyed by ID (the AUTOINC primary key), so we can verify
            // nothing was lost, reordered, or corrupted by the rebuild.
            var beforeById = new Dictionary<int, string[]>();
            int beforeCount;
            using (var table = new ParadoxTableFile(dbPath))
            {
                beforeCount = 0;
                foreach (var rec in table.Enumerate())
                {
                    int id = Convert.ToInt32(rec.DataValues[F_ID]);
                    beforeById[id] = rec.DataValues.Select(FieldValueToComparableString).ToArray();
                    beforeCount++;
                }
            }
            Console.WriteLine("Captured {0} record(s) before rebuild.", beforeCount);

            var beforeFileSizes = Directory.GetFiles(TestFolder, Path.GetFileNameWithoutExtension(TestTabTableName) + ".*")
                .ToDictionary(Path.GetFileName, f => new FileInfo(f).Length);

            Console.WriteLine("=== Rebuild test: rebuilding table ===");
            var result = TableRebuilder.Rebuild(dbPath);
            Console.WriteLine("Rebuild complete: {0} record(s) migrated, {1} file(s) rebuilt.",
                result.RecordsMigrated, result.RebuiltFiles.Count);
            foreach (var f in result.RebuiltFiles)
                Console.WriteLine("  rebuilt: {0}", f);

            bool allOk = true;

            if (result.RecordsMigrated != beforeCount)
            {
                Console.WriteLine("  [FAIL] RecordsMigrated={0}, expected {1}", result.RecordsMigrated, beforeCount);
                allOk = false;
            }

            Console.WriteLine("=== Rebuild test: verifying rebuilt table ===");
            using (var table = new ParadoxTableFile(dbPath))
            {
                var afterById = new Dictionary<int, string[]>();
                int afterCount = 0;
                foreach (var rec in table.Enumerate())
                {
                    int id = Convert.ToInt32(rec.DataValues[F_ID]);
                    afterById[id] = rec.DataValues.Select(FieldValueToComparableString).ToArray();
                    afterCount++;
                }

                if (afterCount != beforeCount)
                {
                    Console.WriteLine("  [FAIL] Record count after rebuild = {0}, expected {1}", afterCount, beforeCount);
                    allOk = false;
                }

                foreach (var kvp in beforeById)
                {
                    if (!afterById.TryGetValue(kvp.Key, out var afterValues))
                    {
                        Console.WriteLine("  [FAIL] Record id={0} missing after rebuild", kvp.Key);
                        allOk = false;
                        continue;
                    }

                    for (int i = 0; i < kvp.Value.Length; i++)
                    {
                        if (kvp.Value[i] != afterValues[i])
                        {
                            Console.WriteLine("  [FAIL] Record id={0} field[{1}] mismatch: before='{2}' after='{3}'",
                                kvp.Key, i, kvp.Value[i], afterValues[i]);
                            allOk = false;
                        }
                    }
                }

                // Primary index lookup still works against the rebuilt .PX.
                var pkHit = table.PrimaryKeyIndex?
                    .Enumerate(new ParadoxCondition.Compare(ParadoxCompareOperator.Equal, 3, F_ID, F_ID))
                    .FirstOrDefault();
                if (pkHit == null)
                {
                    Console.WriteLine("  [FAIL] Primary index lookup for ID=3 returned no result after rebuild.");
                    allOk = false;
                }
                else
                {
                    Console.WriteLine("  [ok] Primary index lookup for ID=3 -> SECVAL={0}", pkHit.DataValues[F_SECVAL]);
                }

                // Secondary index lookup (SECIDX / SECVAL) still works against
                // the rebuilt .XG0/.YG0.
                var secIdx = table.SecondaryIndexes?.FirstOrDefault(ix =>
                    ix.FilePath.EndsWith(".XG0", StringComparison.OrdinalIgnoreCase));
                if (secIdx == null)
                {
                    Console.WriteLine("  [FAIL] Secondary index (SECIDX/.XG0) not found after rebuild.");
                    allOk = false;
                }
                else
                {
                    var secHit = secIdx
                        .Enumerate(new ParadoxCondition.Compare(ParadoxCompareOperator.Equal, (short)3, F_SECVAL, 0))
                        .FirstOrDefault();
                    if (secHit == null)
                    {
                        Console.WriteLine("  [FAIL] Secondary index lookup for SECVAL=3 returned no result after rebuild.");
                        allOk = false;
                    }
                    else
                    {
                        Console.WriteLine("  [ok] Secondary index lookup for SECVAL=3 -> ID={0}", secHit.DataValues[F_ID]);
                    }
                }
            }

            var afterFileSizes = Directory.GetFiles(TestFolder, Path.GetFileNameWithoutExtension(TestTabTableName) + ".*")
                .ToDictionary(Path.GetFileName, f => new FileInfo(f).Length);
            foreach (var kvp in beforeFileSizes)
            {
                afterFileSizes.TryGetValue(kvp.Key, out var afterSize);
                Console.WriteLine("  {0}: before={1} bytes, after={2} bytes", kvp.Key, kvp.Value, afterSize);
            }

            Console.WriteLine();
            Console.WriteLine(allOk ? "Rebuild test PASSED." : "Rebuild test FAILED.");
        }

        /// <summary>
        /// Diagnostic-only: stages testtab_indexoutofdate.DB (whose .PX/.Xnn
        /// index files still reflect the empty TESTTAB.DB, while the .DB/.MB
        /// files themselves hold the fully-populated post-append/post-update
        /// data) and reports what happens when opening it and using its
        /// indexes.
        ///
        /// Expected behavior: opening the table succeeds and flags
        /// ParadoxTableFile.IndexOutOfDate = true (mirroring BDE, which
        /// still lets you open/scan a table with a stale index); any
        /// subsequent attempt to actually read/write via the stale index
        /// (PK lookup, secondary index lookup, or any insert/update/delete
        /// that touches the index) throws IndexOutOfDateException. A plain
        /// full-table scan (no index) is unaffected.
        /// </summary>
        private static void RunIndexOutOfDateTest()
        {
            ResetTestFolder();

            string baseName = "testtab_indexoutofdate";
            foreach (var src in Directory.GetFiles(SourceDataFolder, baseName + ".*"))
                File.Copy(src, Path.Combine(TestFolder, Path.GetFileName(src)), overwrite: true);

            string dbPath = Path.Combine(TestFolder, baseName + ".DB");

            bool allOk = true;

            Console.WriteLine("=== IndexOutOfDate test: opening table ===");
            try
            {
                using (var table = new ParadoxTableFile(dbPath))
                {
                    Console.WriteLine("  [ok] Opened table.");

                    if (!table.IndexOutOfDate)
                    {
                        Console.WriteLine("  [FAIL] ParadoxTableFile.IndexOutOfDate was false; expected true.");
                        allOk = false;
                    }
                    else
                    {
                        Console.WriteLine("  [ok] ParadoxTableFile.IndexOutOfDate = true, as expected.");
                    }

                    Console.WriteLine("=== IndexOutOfDate test: full scan (no index) ===");
                    int count = 0;
                    foreach (var rec in table.Enumerate()) count++;
                    Console.WriteLine("  [ok] Full scan: {0} record(s) (unaffected by stale index).", count);

                    Console.WriteLine("=== IndexOutOfDate test: primary index lookup (should throw) ===");
                    try
                    {
                        var pkHit = table.PrimaryKeyIndex?
                            .Enumerate(new ParadoxCondition.Compare(ParadoxCompareOperator.Equal, 3, F_ID, F_ID))
                            .FirstOrDefault();
                        Console.WriteLine("  [FAIL] PK lookup did not throw; returned {0}.", pkHit != null ? "found" : "not found");
                        allOk = false;
                    }
                    catch (IndexOutOfDateException ex)
                    {
                        Console.WriteLine("  [ok] PK lookup threw IndexOutOfDateException: {0}", ex.Message);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("  [FAIL] PK lookup threw unexpected {0}: {1}", ex.GetType().Name, ex.Message);
                        allOk = false;
                    }

                    Console.WriteLine("=== IndexOutOfDate test: append (write path, should throw) ===");
                    try
                    {
                        var values = BuildAppendValues(table);
                        table.AppendRecord(values);
                        Console.WriteLine("  [FAIL] Append did not throw.");
                        allOk = false;
                    }
                    catch (IndexOutOfDateException ex)
                    {
                        Console.WriteLine("  [ok] Append threw IndexOutOfDateException: {0}", ex.Message);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("  [FAIL] Append threw unexpected {0}: {1}", ex.GetType().Name, ex.Message);
                        allOk = false;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Opening table threw: {0}: {1}", ex.GetType().Name, ex.Message);
                allOk = false;
            }

            Console.WriteLine();
            Console.WriteLine(allOk ? "IndexOutOfDate test PASSED." : "IndexOutOfDate test FAILED.");
        }

        private static object[] BuildAppendValues(ParadoxTableFile table)
        {
            var values = new object[table.FieldCount];
            return values;
        }

        private static string FieldValueToComparableString(object value)
        {
            switch (value)
            {
                case null:
                    return string.Empty;
                case MemoValue mv:
                    return mv.Text ?? string.Empty;
                case byte[] bytes:
                    return BitConverter.ToString(bytes);
                case DateTime dt:
                    return dt.ToString("O");
                default:
                    return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
            }
        }

        // --------------------------------------------------------------------
        // Stepwise reproduction: SQLRunner-only mode vs. harness-only mode
        // --------------------------------------------------------------------
        //
        // Both modes perform the *same logical sequence* of operations
        // (append x5, update secval=3, insert id=6, delete secval=5, then the
        // sentinel verify statements that historically caused SQLRunner to
        // hang) but one path uses SQLRunner exclusively (raw BDE Local SQL)
        // and the other uses ParadoxReader exclusively. After each step, the
        // table's .DB/.MB files are snapshotted to disk (StepSnapshotRoot)
        // labeled with the step number/name and which mode produced it, so
        // `comparesteps` can byte-compare them afterward to find exactly
        // where behavior/corruption diverges, and which step SQLRunner hangs
        // on.

        private const string StepSnapshotRoot = @"c:\temp\cmp\steps";

        private static void SnapshotStep(string modeTag, int stepNumber, string stepName)
        {
            var destDir = Path.Combine(StepSnapshotRoot, modeTag);
            Directory.CreateDirectory(destDir);

            var tableBaseName = Path.GetFileNameWithoutExtension(TestTabTableName);
            foreach (var srcFile in Directory.GetFiles(TestFolder, tableBaseName + ".*"))
            {
                var ext = Path.GetExtension(srcFile);
                var destFile = Path.Combine(destDir, $"{stepNumber:D2}_{stepName}{ext}");
                try { File.Copy(srcFile, destFile, overwrite: true); }
                catch (Exception ex) { Console.WriteLine("  [warn] snapshot copy failed for {0}: {1}", srcFile, ex.Message); }
            }

            Console.WriteLine("  [snapshot] {0} step {1:D2} ({2}) captured -> {3}", modeTag, stepNumber, stepName, destDir);
        }

        /// <summary>
        /// Runs ONLY the equivalent BDE Local SQL statements via SQLRunner,
        /// with no ParadoxReader involvement at all, taking a snapshot of the
        /// table files after each step. Intended to be run up to (and
        /// including) the point where SQLRunner previously hung, so that
        /// point can be pinpointed independent of the ParadoxReader harness.
        /// </summary>
        private static void RunSqlRunnerOnlyMode()
        {
            if (!File.Exists(SqlRunnerExePath))
            {
                Console.WriteLine("SQLRunner not found at {0}; aborting.", SqlRunnerExePath);
                return;
            }

            ResetTestFolder();
            var tablePath = Path.Combine(TestFolder, TestTabTableName);
            int step = 0;

            Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: baseline (post-reset) ===", ++step);
            SnapshotStep("sqlrunner", step, "baseline");

            for (int i = 1; i <= 5; i++)
            {
                var v = MakeSampleFieldValues((short)i, "append" + i);
                Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: append secval={1} ===", ++step, i);
                RunSqlRunner(BuildInsertSql(tablePath, v));
                SnapshotStep("sqlrunner", step, $"append_secval{i}");
            }

            Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: update secval=3 ===", ++step);
            RunSqlRunner($"UPDATE '{tablePath}' T SET T.'CHARVAL' = 'CHAR-updated', T.'INTVAL' = 999999, T.'MONEYVAL' = 1234.56 WHERE T.'SECVAL' = 3");
            SnapshotStep("sqlrunner", step, "update_secval3");

            Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: insert secval=6 ===", ++step);
            var v6 = MakeSampleFieldValues(6, "insert6");
            RunSqlRunner(BuildInsertSql(tablePath, v6));
            SnapshotStep("sqlrunner", step, "insert_secval6");

            Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: delete secval=5 ===", ++step);
            RunSqlRunner($"DELETE FROM '{tablePath}' T WHERE T.'SECVAL' = 5");
            SnapshotStep("sqlrunner", step, "delete_secval5");

            // These are the exact sentinel statements from TestVerifyWithSqlRunner,
            // run one at a time here so a hang can be attributed to a specific
            // statement instead of the batch as a whole.
            Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: verify UPDATE id=-999999 (no rows) ===", ++step);
            RunSqlRunner($"UPDATE '{tablePath}' T SET T.'CHARVAL' = T.'CHARVAL' WHERE T.'ID' = -999999");
            SnapshotStep("sqlrunner", step, "verify_update_noop");

            Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: verify UPDATE secval=-32000 (no rows) ===", ++step);
            RunSqlRunner($"UPDATE '{tablePath}' T SET T.'INTVAL' = T.'INTVAL' WHERE T.'SECVAL' = -32000");
            SnapshotStep("sqlrunner", step, "verify_update_sentinel");

            Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: verify INSERT secval=-1 ===", ++step);
            RunSqlRunner($"INSERT INTO '{tablePath}' (SECVAL) VALUES (-1)");
            SnapshotStep("sqlrunner", step, "verify_insert_sentinel");

            Console.WriteLine("=== [sqlrunnermode] Step {0:D2}: verify DELETE secval=-1 ===", ++step);
            RunSqlRunner($"DELETE FROM '{tablePath}' T WHERE T.'SECVAL' = -1");
            SnapshotStep("sqlrunner", step, "verify_delete_sentinel");

            Console.WriteLine("[sqlrunnermode] Completed all {0} steps without hanging.", step);
        }

        /// <summary>
        /// Runs ONLY ParadoxReader operations (no SQLRunner at all) for the
        /// same logical sequence as RunSqlRunnerOnlyMode, snapshotting the
        /// table after each step, so `comparesteps` can byte-diff the two
        /// step sequences to find where they first disagree.
        /// </summary>
        private static void RunHarnessOnlyMode()
        {
            ResetTestFolder();
            int step = 0;

            Console.WriteLine("=== [harness] Step {0:D2}: baseline (post-reset) ===", ++step);
            SnapshotStep("harness", step, "baseline");

            for (int i = 1; i <= 5; i++)
            {
                Console.WriteLine("=== [harness] Step {0:D2}: append secval={1} ===", ++step, i);
                using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
                {
                    var values = MakeSampleFieldValues((short)i, "append" + i);
                    table.AppendRecord(values);
                }
                SnapshotStep("harness", step, $"append_secval{i}");
            }

            Console.WriteLine("=== [harness] Step {0:D2}: update secval=3 ===", ++step);
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                ParadoxRecord target = null;
                foreach (var rec in table.Enumerate())
                {
                    if (Convert.ToInt32(rec.DataValues[F_SECVAL]) == 3) { target = rec; break; }
                }
                if (target != null)
                {
                    var newValues = target.CloneDataValues();
                    newValues[F_CHARVAL]  = "CHAR-updated";
                    newValues[F_INTVAL]   = 999999;
                    newValues[F_MONEYVAL] = 1234.56m;
                    newValues[F_MEMOVAL]  = new MemoValue("updated memo text", (newValues[F_MEMOVAL] as MemoValue)?.BlobInfo);
                    table.UpdateRecord(target, newValues);
                }
            }
            SnapshotStep("harness", step, "update_secval3");

            Console.WriteLine("=== [harness] Step {0:D2}: insert id=6 ===", ++step);
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                var values = MakeSampleFieldValues(6, "insert6");
                values[F_ID] = 6;
                table.InsertRecord(values);
            }
            SnapshotStep("harness", step, "insert_id6");

            Console.WriteLine("=== [harness] Step {0:D2}: delete secval=5 ===", ++step);
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                ParadoxRecord target = null;
                foreach (var rec in table.Enumerate())
                {
                    if (Convert.ToInt32(rec.DataValues[F_SECVAL]) == 5) { target = rec; break; }
                }
                if (target != null) table.DeleteRecord(target);
            }
            SnapshotStep("harness", step, "delete_secval5");

            // The "verify" steps in SQLRunner-only mode had no ParadoxReader
            // side; harness mode just re-snapshots the unchanged table at
            // each corresponding step number so the two sequences line up
            // 1:1 for comparison.
            for (int verifyStep = 1; verifyStep <= 4; verifyStep++)
            {
                var names = new[] { "verify_update_noop", "verify_update_sentinel", "verify_insert_sentinel", "verify_delete_sentinel" };
                Console.WriteLine("=== [harness] Step {0:D2}: {1} (no-op on harness side) ===", ++step, names[verifyStep - 1]);
                SnapshotStep("harness", step, names[verifyStep - 1]);
            }

            Console.WriteLine("[harness] Completed all {0} steps.", step);
        }

        /// <summary>
        /// Builds a raw INSERT statement equivalent to MakeSampleFieldValues,
        /// for use by RunSqlRunnerOnlyMode. AUTOINC (ID) is set explicitly so
        /// both sequences end up with matching ID values.
        /// </summary>
        private static string BuildInsertSql(string tablePath, object[] v)
        {
            string Esc(string s) => s.Replace("'", "''");
            var dateVal      = (DateTime)v[F_DATEVAL];
            var timestampVal = (DateTime)v[F_TIMESTAMPVAL];
            var timeVal      = (TimeSpan)v[F_TIMEVAL];

            // Deliberately omit the AUTOINC (ID) column: explicitly setting
            // it in an INSERT causes SQLRunner/BDE to silently no-op (report
            // "Successfully executed" but not actually add the row - verified
            // by checking the .DB header RecordCount before/after). AUTOINC
            // must be left for BDE to assign, matching how ParadoxReader's
            // AppendRecord auto-assigns IDs too.
            return "INSERT INTO '" + tablePath + "' " +
                "(SECVAL, INTVAL, DECVAL, NUMVAL, FLOATVAL, CHARVAL, VARCHARVAL, DATEVAL, BOOLVAL, TIMEVAL, TIMESTAMPVAL, MONEYVAL) VALUES (" +
                v[F_SECVAL] + ", " +
                v[F_INTVAL] + ", " +
                Convert.ToDecimal(v[F_DECVAL]).ToString(System.Globalization.CultureInfo.InvariantCulture) + ", " +
                Convert.ToDecimal(v[F_NUMVAL]).ToString(System.Globalization.CultureInfo.InvariantCulture) + ", " +
                Convert.ToDouble(v[F_FLOATVAL]).ToString(System.Globalization.CultureInfo.InvariantCulture) + ", " +
                "'" + Esc((string)v[F_CHARVAL]) + "', " +
                "'" + Esc((string)v[F_VARCHARVAL]) + "', " +
                "'" + dateVal.ToString("MM/dd/yyyy") + "', " +
                ((bool)v[F_BOOLVAL] ? "TRUE" : "FALSE") + ", " +
                "'" + timeVal.Hours.ToString("00") + ":" + timeVal.Minutes.ToString("00") + ":" + timeVal.Seconds.ToString("00") + "', " +
                "'" + timestampVal.ToString("MM/dd/yyyy HH:mm:ss") + "', " +
                Convert.ToDecimal(v[F_MONEYVAL]).ToString(System.Globalization.CultureInfo.InvariantCulture) +
                ")";
        }

        /// <summary>
        /// Byte-compares the step snapshots captured by RunSqlRunnerOnlyMode
        /// and RunHarnessOnlyMode (StepSnapshotRoot\sqlrunner vs \harness),
        /// step-by-step, reporting the first step (if any) where the .DB/.MB
        /// bytes diverge (ignoring known-volatile header bytes is NOT done
        /// here - this is a raw diff so header change-counters will always
        /// show as different; focus on whether the *data* differs).
        /// </summary>
        // Header decode/diff logic moved to DbHeaderSnapshot.cs (proper,
        // reusable diagnostic logic rather than test code); it deliberately
        // mirrors ParadoxReader.ParadoxFile.ReadHeader's offsets as an
        // independent cross-check.

        private static void CompareStepSnapshots()
        {
            var srDir = Path.Combine(StepSnapshotRoot, "sqlrunner");
            var hDir  = Path.Combine(StepSnapshotRoot, "harness");

            if (!Directory.Exists(srDir) || !Directory.Exists(hDir))
            {
                Console.WriteLine("Missing snapshot directories. Run 'sqlrunnermode' and 'harnessmode' first.");
                Console.WriteLine("  sqlrunner dir exists: {0} ({1})", Directory.Exists(srDir), srDir);
                Console.WriteLine("  harness dir exists:   {0} ({1})", Directory.Exists(hDir), hDir);
                return;
            }

            // Match files by step-number prefix + extension (e.g. "08_" + ".DB")
            // rather than exact filename, since the two modes' step labels can
            // legitimately differ in wording (e.g. sqlrunner's
            // "08_insert_secval6.DB" vs harness's "08_insert_id6.DB") while
            // still representing the same logical step.
            string StepKey(string fileName)
            {
                var name = Path.GetFileNameWithoutExtension(fileName);
                var ext  = Path.GetExtension(fileName);
                var underscoreIdx = name.IndexOf('_');
                var prefix = underscoreIdx >= 0 ? name.Substring(0, underscoreIdx) : name;
                return prefix + ext;
            }

            var srFiles = Directory.GetFiles(srDir).Select(Path.GetFileName).ToList();
            var hFiles  = Directory.GetFiles(hDir).Select(Path.GetFileName).ToList();

            var srByKey = srFiles.GroupBy(StepKey).ToDictionary(g => g.Key, g => g.First());
            var hByKey  = hFiles.GroupBy(StepKey).ToDictionary(g => g.Key, g => g.First());

            var allKeys = srByKey.Keys.Union(hByKey.Keys).OrderBy(k => k, StringComparer.Ordinal).ToList();

            bool anyDiff = false;
            foreach (var key in allKeys)
            {
                if (!srByKey.TryGetValue(key, out var srName)) { Console.WriteLine("[{0}] MISSING on sqlrunner side", key); anyDiff = true; continue; }
                if (!hByKey.TryGetValue(key, out var hName))   { Console.WriteLine("[{0}] MISSING on harness side", key); anyDiff = true; continue; }

                var name = srName == hName ? srName : $"{srName} <-> {hName}";
                var srPath = Path.Combine(srDir, srName);
                var hPath  = Path.Combine(hDir, hName);

                var srBytes = File.ReadAllBytes(srPath);
                var hBytes  = File.ReadAllBytes(hPath);

                if (srBytes.Length != hBytes.Length)
                {
                    Console.WriteLine("[{0}] SIZE MISMATCH: sqlrunner={1} bytes, harness={2} bytes", name, srBytes.Length, hBytes.Length);
                    anyDiff = true;

                    if (name.EndsWith(".DB", StringComparison.OrdinalIgnoreCase))
                    {
                        try
                        {
                            var srHdr = DbHeaderSnapshot.Read(srPath);
                            var hHdr  = DbHeaderSnapshot.Read(hPath);
                            foreach (var d in srHdr.DiffAgainst(hHdr))
                                Console.WriteLine("    header diff: {0}", d);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("    [warn] could not decode headers: {0}", ex.Message);
                        }
                    }
                    continue;
                }

                var diffOffsets = new List<int>();
                for (int i = 0; i < srBytes.Length; i++)
                {
                    if (srBytes[i] != hBytes[i]) diffOffsets.Add(i);
                }

                if (diffOffsets.Count == 0)
                {
                    Console.WriteLine("[{0}] MATCH ({1} bytes)", name, srBytes.Length);
                }
                else
                {
                    // Some header bytes are known to hold raw in-memory
                    // pointer values from whichever process last wrote the
                    // file (BDE/SQLRunner vs. ParadoxReader), not persisted
                    // table state, so they are expected to differ between
                    // the two engines even when the actual table state is
                    // identical: unknown12x13 (0x12-0x13), unknownPtr1A /
                    // pointer (0x1A-0x1D), tableNamePtrPtr (0x30-0x33),
                    // fldInfoPtr (0x34-0x37). Filter those out to see if any
                    // *meaningful* bytes differ underneath.
                    bool IsKnownVolatile(int offset) =>
                        (offset >= 0x12 && offset <= 0x13) ||
                        (offset >= 0x1A && offset <= 0x1D) ||
                        (offset >= 0x30 && offset <= 0x33) ||
                        (offset >= 0x34 && offset <= 0x37);

                    var meaningfulOffsets = name.EndsWith(".DB", StringComparison.OrdinalIgnoreCase)
                        ? diffOffsets.Where(o => !IsKnownVolatile(o)).ToList()
                        : diffOffsets;

                    anyDiff = true;
                    var first = diffOffsets.Take(10).Select(o => $"0x{o:X} (sr={srBytes[o]:X2} h={hBytes[o]:X2})");
                    Console.WriteLine("[{0}] DIFF: {1} byte(s) differ ({2} after filtering known-volatile pointer bytes). First offsets: {3}{4}",
                        name, diffOffsets.Count, meaningfulOffsets.Count, string.Join(", ", first.ToArray()), diffOffsets.Count > 10 ? ", ..." : "");

                    if (meaningfulOffsets.Count > 0 && meaningfulOffsets.Count != diffOffsets.Count)
                    {
                        var firstMeaningful = meaningfulOffsets.Take(10).Select(o => $"0x{o:X} (sr={srBytes[o]:X2} h={hBytes[o]:X2})");
                        Console.WriteLine("    meaningful (non-pointer) offsets: {0}{1}",
                            string.Join(", ", firstMeaningful.ToArray()), meaningfulOffsets.Count > 10 ? ", ..." : "");
                    }

                    if (name.EndsWith(".DB", StringComparison.OrdinalIgnoreCase))
                    {
                        try
                        {
                            var srHdr = DbHeaderSnapshot.Read(srPath);
                            var hHdr  = DbHeaderSnapshot.Read(hPath);
                            var headerDiffs = srHdr.DiffAgainst(hHdr).ToList();
                            if (headerDiffs.Count == 0)
                                Console.WriteLine("    header fields match; diff is confined to record/data area.");
                            else
                                foreach (var d in headerDiffs)
                                    Console.WriteLine("    header diff: {0}", d);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("    [warn] could not decode headers: {0}", ex.Message);
                        }
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine(anyDiff ? "Comparison complete: differences found (see above)." : "Comparison complete: all matched files are byte-identical.");
        }

        // --------------------------------------------------------------------
        // Setup
        // --------------------------------------------------------------------

        /// <summary>
        /// Wipes and recreates c:\temp\paradoxtest, then copies a fresh copy of
        /// TESTTAB.DB (and its associated .PX/.MB and secondary index files) from
        /// the build output's data folder, so every run starts from a known state.
        /// </summary>
        private static void ResetTestFolder()
        {
            Directory.CreateDirectory(TestFolder);

            // Clear stale *.LCK files left behind by a prior SQLRunner/BDE
            // session (e.g. PDOXUSRS.LCK) before touching anything else.
            DeleteStaleLockFiles();

            // Delete individual table/index/blob files rather than the whole
            // directory: BDE (via SQLRunner) leaves behind a PDOXUSRS.LCK
            // network-lock placeholder file in the folder that can't always be
            // removed immediately, so a recursive directory delete can fail.
            var tableBaseName = Path.GetFileNameWithoutExtension(TestTabTableName);
            foreach (var existingFile in Directory.GetFiles(TestFolder, tableBaseName + ".*"))
            {
                try { File.Delete(existingFile); } catch { /* best effort */ }
            }

            foreach (var sourceFile in Directory.GetFiles(SourceDataFolder, tableBaseName + ".*"))
            {
                var destFile = Path.Combine(TestFolder, Path.GetFileName(sourceFile));
                File.Copy(sourceFile, destFile, overwrite: true);
            }

            Console.WriteLine("Test folder reset: {0}", TestFolder);
        }

        private static object[] MakeSampleFieldValues(short secVal, string label)
        {
            // Kept intentionally simple (small integers, 2-decimal-place
            // money/decimal/numeric values) to make byte-for-byte comparison
            // against SQLRunner/BDE output easier to reason about.
            var values = new object[15];
            values[F_ID]           = null; // AUTOINC - assigned automatically by AppendRecord
            values[F_SECVAL]       = secVal;
            values[F_INTVAL]       = 100 + secVal;
            values[F_DECVAL]       = Math.Round(10m + secVal, 2);
            values[F_NUMVAL]       = Math.Round(20m + secVal, 2);
            values[F_FLOATVAL]     = Math.Round(1.5d + secVal, 2);
            values[F_CHARVAL]      = "CHAR-" + label;
            values[F_VARCHARVAL]   = "VARCHAR-" + label;
            values[F_DATEVAL]      = new DateTime(2026, 1, 1).AddDays(secVal);
            values[F_BOOLVAL]      = secVal % 2 == 0;
            values[F_MEMOVAL]      = new MemoValue("memo text for " + label, null);
            values[F_TIMEVAL]      = new TimeSpan(1, 2, 3);
            values[F_TIMESTAMPVAL] = new DateTime(2026, 1, 1, 12, 0, 0).AddMinutes(secVal);
            values[F_MONEYVAL]     = Math.Round(100m + secVal, 2);
            values[F_BYTESVAL]     = new byte[] { (byte)secVal, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            return values;
        }

        // --------------------------------------------------------------------
        // Test 1: Append
        // --------------------------------------------------------------------

        private static void TestAppendRecords()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                for (short i = 1; i <= 5; i++)
                {
                    var values = MakeSampleFieldValues(i, "append" + i);
                    var rec    = table.AppendRecord(values);
                    Console.WriteLine("Appended record: block={0} idx={1} id={2} secval={3}",
                        rec.BlockNumber, rec.RecordIndex, rec.DataValues[F_ID], rec.DataValues[F_SECVAL]);
                }
            }
        }

        // --------------------------------------------------------------------
        // Test 2: Update
        // --------------------------------------------------------------------

        private static void TestUpdateRecord()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                ParadoxRecord target = null;
                foreach (var rec in table.Enumerate())
                {
                    if (Convert.ToInt32(rec.DataValues[F_SECVAL]) == 3)
                    {
                        target = rec;
                        break;
                    }
                }

                if (target == null)
                {
                    Console.WriteLine("No record with SECVAL=3 found to update.");
                    return;
                }

                var newValues = target.CloneDataValues();
                newValues[F_CHARVAL]    = "CHAR-updated";
                newValues[F_INTVAL]     = 999999;
                newValues[F_MONEYVAL]   = 1234.56m;
                newValues[F_MEMOVAL]    = new MemoValue("updated memo text", (newValues[F_MEMOVAL] as MemoValue)?.BlobInfo);

                table.UpdateRecord(target, newValues);
                Console.WriteLine("Updated record id={0} (secval=3): charval={1}, intval={2}",
                    newValues[F_ID], newValues[F_CHARVAL], newValues[F_INTVAL]);
            }
        }

        // --------------------------------------------------------------------
        // Test 3: Insert
        // --------------------------------------------------------------------

        private static void TestInsertRecord()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                var values = MakeSampleFieldValues(6, "insert6");
                values[F_ID] = 6; // InsertRecord does not auto-assign AUTOINC
                var rec = table.InsertRecord(values);
                Console.WriteLine("Inserted record: block={0} idx={1} id={2} secval={3}",
                    rec.BlockNumber, rec.RecordIndex, rec.DataValues[F_ID], rec.DataValues[F_SECVAL]);
            }
        }

        // --------------------------------------------------------------------
        // Test 4: Delete
        // --------------------------------------------------------------------

        private static void TestDeleteRecord()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                ParadoxRecord target = null;
                foreach (var rec in table.Enumerate())
                {
                    if (Convert.ToInt32(rec.DataValues[F_SECVAL]) == 5)
                    {
                        target = rec;
                        break;
                    }
                }

                if (target == null)
                {
                    Console.WriteLine("No record with SECVAL=5 found to delete.");
                    return;
                }

                table.DeleteRecord(target);
                Console.WriteLine("Deleted record id={0} (secval=5)", target.DataValues[F_ID]);
            }
        }

        // --------------------------------------------------------------------
        // Test 5: Read back
        // --------------------------------------------------------------------

        private static void TestReadAllRecords()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                int count = 0;
                foreach (var rec in table.Enumerate())
                {
                    count++;
                    Console.WriteLine("Record #{0}: id={1} secval={2} charval={3} intval={4} moneyval={5}",
                        count,
                        rec.DataValues[F_ID],
                        rec.DataValues[F_SECVAL],
                        rec.DataValues[F_CHARVAL],
                        rec.DataValues[F_INTVAL],
                        rec.DataValues[F_MONEYVAL]);
                }
                Console.WriteLine("Total records: {0}", count);
            }
        }

        // --------------------------------------------------------------------
        // Test 5b: ParadoxCondition-driven primary key (.PX) index lookup,
        // read via ParadoxDataReader (IDataReader), matching the pattern used
        // in an earlier version of this codebase:
        //
        //     using (var index = table.PrimaryKeyIndex)
        //     {
        //         var condition = new ParadoxCondition.LogicalAnd(...);
        //         var qry = index.Enumerate(condition);
        //         using (var rdr = new ParadoxDataReader(table, qry))
        //         {
        //             while (rdr.Read()) { ... }
        //         }
        //     }
        //
        // Note: PrimaryKeyIndex is owned/disposed by ParadoxTableFile itself
        // (see ParadoxTableFile.Dispose), so it is not re-wrapped in its own
        // "using" here.
        //
        // NOTE: secondary index (.Xnn/.Xgn/.Ynn/.Ygn) lookups follow the same
        // pattern but through table.SecondaryIndexes[n].Enumerate(condition) -
        // see TestConditionSecondaryIndexLookup below.
        // --------------------------------------------------------------------

        private static void TestConditionIndexLookup()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                var index = table.PrimaryKeyIndex;
                if (index == null)
                {
                    Console.WriteLine("  [skip] No .PX file open for this table; cannot test index lookup.");
                    return;
                }

                // ID (F_ID) is the primary key field, so a range condition on
                // it can be evaluated against the .PX index directly.
                var condition =
                    new ParadoxCondition.LogicalAnd(
                        new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, 2, F_ID, 0),
                        new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, 4, F_ID, 0));

                var qry = index.Enumerate(condition);
                int recIndex = 1;
                using (var rdr = new ParadoxDataReader(table, qry))
                {
                    while (rdr.Read())
                    {
                        Console.WriteLine("Record #{0}", recIndex);
                        for (int i = 0; i < rdr.FieldCount; i++)
                        {
                            Console.WriteLine("    {0} = {1}", rdr.GetName(i), rdr[i]);
                        }

                        if (++recIndex > 10) { break; }
                    }
                }
                Console.WriteLine("  (2 <= ID <= 4) matched {0} record(s) via .PX index lookup.", recIndex - 1);
            }
        }

        // --------------------------------------------------------------------
        // Test 5c/5d/5e: Condition + secondary index lookup
        // --------------------------------------------------------------------

        /// <summary>
        /// Finds the first open secondary index whose composed key starts
        /// with <paramref name="leadingFieldIndex"/> (i.e. it is the index's
        /// primary/leading indexed field, not just incidentally present via
        /// the appended primary-key suffix). Returns null if no such index
        /// is open (e.g. table copied without its .Xnn/.Xgn companions).
        /// </summary>
        private static SecondaryIndexHandle FindSecondaryIndexByLeadingField(ParadoxTableFile table, int leadingFieldIndex)
        {
            foreach (var idx in table.SecondaryIndexes)
            {
                if (idx.FieldIndices.Length > 0 && idx.FieldIndices[0] == leadingFieldIndex)
                    return idx;
            }
            return null;
        }

        /// <summary>
        /// Mirrors <see cref="TestConditionIndexLookup"/> but exercises a
        /// secondary index (.Xnn/.Xgn) via <see cref="SecondaryIndexHandle.Enumerate"/>,
        /// using SECVAL (the field covered by the SECIDX secondary index)
        /// instead of the primary key.
        /// </summary>
        private static void TestConditionSecondaryIndexLookup()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                var index = FindSecondaryIndexByLeadingField(table, F_SECVAL);
                if (index == null)
                {
                    Console.WriteLine("  [skip] No secondary index over SECVAL (SECIDX) is open for this table; cannot test index lookup.");
                    return;
                }

                // The index's composed key layout may differ from the parent
                // table's field layout, so map F_SECVAL's table position to
                // its position within this index's own key via FieldIndices.
                int secValIndexPos = Array.IndexOf(index.FieldIndices, F_SECVAL);

                var condition =
                    new ParadoxCondition.LogicalAnd(
                        new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, (short)2, F_SECVAL, secValIndexPos),
                        new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, (short)4, F_SECVAL, secValIndexPos));

                var qry = index.Enumerate(condition);
                int recIndex = 1;
                using (var rdr = new ParadoxDataReader(table, qry))
                {
                    while (rdr.Read())
                    {
                        Console.WriteLine("Record #{0}", recIndex);
                        for (int i = 0; i < rdr.FieldCount; i++)
                        {
                            Console.WriteLine("    {0} = {1}", rdr.GetName(i), rdr[i]);
                        }

                        if (++recIndex > 10) { break; }
                    }
                }
                Console.WriteLine("  (2 <= SECVAL <= 4) matched {0} record(s) via {1} secondary index lookup.", recIndex - 1, Path.GetFileName(index.FilePath));
            }
        }

        /// <summary>
        /// Exercises the IDXCHAR secondary index (CHARVAL), validating the
        /// Alpha-type key encode/decode/compare path through
        /// KeySerializer.Deserialize and KeySerializer's Alpha comparison
        /// (ordinal, trimmed of trailing nulls) - untested by the numeric
        /// SECVAL index above.
        /// </summary>
        private static void TestConditionSecondaryIndexLookupCharVal()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                var index = FindSecondaryIndexByLeadingField(table, F_CHARVAL);
                if (index == null)
                {
                    Console.WriteLine("  [skip] No secondary index over CHARVAL (IDXCHAR) is open for this table; cannot test index lookup.");
                    return;
                }

                int charValIndexPos = Array.IndexOf(index.FieldIndices, F_CHARVAL);

                // MakeSampleFieldValues produces CHARVAL = "CHAR-append" + secVal
                // (plus "CHAR-updated" for the row updated in Test 2), so an
                // equality match on one specific appended value is a simple,
                // deterministic check of the Alpha index path.
                var condition =
                    new ParadoxCondition.Compare(ParadoxCompareOperator.Equal, "CHAR-append2", F_CHARVAL, charValIndexPos);

                var qry = index.Enumerate(condition);
                int recIndex = 1;
                using (var rdr = new ParadoxDataReader(table, qry))
                {
                    while (rdr.Read())
                    {
                        Console.WriteLine("Record #{0}", recIndex);
                        for (int i = 0; i < rdr.FieldCount; i++)
                        {
                            Console.WriteLine("    {0} = {1}", rdr.GetName(i), rdr[i]);
                        }

                        if (++recIndex > 10) { break; }
                    }
                }
                Console.WriteLine("  (CHARVAL = 'CHAR-append2') matched {0} record(s) via {1} secondary index lookup.", recIndex - 1, Path.GetFileName(index.FilePath));
            }
        }

        /// <summary>
        /// Exercises the IDXCOMPOSITE secondary index (INTVAL, DATEVAL),
        /// validating a true multi-indexed-field composite key (as opposed
        /// to SECIDX/IDXCHAR, which each have a single indexed field plus
        /// the appended primary key). Only the leading indexed field
        /// (INTVAL) is used for pruning here, matching how IsIndexPossible
        /// evaluates a single Compare condition against the composed key.
        /// </summary>
        private static void TestConditionSecondaryIndexLookupComposite()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                var index = FindSecondaryIndexByLeadingField(table, F_INTVAL);
                if (index == null)
                {
                    Console.WriteLine("  [skip] No secondary index over INTVAL (IDXCOMPOSITE) is open for this table; cannot test index lookup.");
                    return;
                }

                int intValIndexPos = Array.IndexOf(index.FieldIndices, F_INTVAL);

                // MakeSampleFieldValues sets INTVAL = 100 + secVal, so this
                // range covers secVal in [2, 4], same rows as Test 5c.
                var condition =
                    new ParadoxCondition.LogicalAnd(
                        new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, 102, F_INTVAL, intValIndexPos),
                        new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, 104, F_INTVAL, intValIndexPos));

                var qry = index.Enumerate(condition);
                int recIndex = 1;
                using (var rdr = new ParadoxDataReader(table, qry))
                {
                    while (rdr.Read())
                    {
                        Console.WriteLine("Record #{0}", recIndex);
                        for (int i = 0; i < rdr.FieldCount; i++)
                        {
                            Console.WriteLine("    {0} = {1}", rdr.GetName(i), rdr[i]);
                        }

                        if (++recIndex > 10) { break; }
                    }
                }
                Console.WriteLine("  (102 <= INTVAL <= 104) matched {0} record(s) via {1} secondary index lookup.", recIndex - 1, Path.GetFileName(index.FilePath));
            }
        }

        // --------------------------------------------------------------------
        // Test 6: Cross-check with SQLRunner
        // --------------------------------------------------------------------

        /// <summary>
        /// Runs BDE Local SQL statements through SQLRunner against the same
        /// table that ParadoxReader just wrote to, to independently confirm
        /// BDE can open/parse the table (and thus that the .DB/.PX files are
        /// structurally valid) after our write-path changes.
        ///
        /// NOTE: SQLRunner only supports UPDATE/INSERT/DELETE/CREATE queries
        /// (its own /? help confirms SELECT is rejected as "Invalid Query"),
        /// so verification here uses harmless UPDATE/INSERT/DELETE statements
        /// against sentinel values that don't match any real row. If BDE can
        /// open and execute these without reporting corruption, the table is
        /// structurally sound from BDE's point of view.
        /// </summary>
        private static void TestVerifyWithSqlRunner()
        {
            if (!File.Exists(SqlRunnerExePath))
            {
                Console.WriteLine("SQLRunner not found at {0}; skipping BDE verification.", SqlRunnerExePath);
                return;
            }

            var tablePath = Path.Combine(TestFolder, TestTabTableName);

            // Touches zero rows (no ID = -999999) but forces BDE to open, parse,
            // and use the .PX index to seek - validating header/index integrity.
            RunSqlRunner($"UPDATE '{tablePath}' T SET T.'CHARVAL' = T.'CHARVAL' WHERE T.'ID' = -999999");

            // SECVAL is SMALLINT (range -32768..32767) - a sentinel outside that
            // range (e.g. -999999) causes BDE/SQLRunner to hang rather than
            // return "no rows found", so use an in-range sentinel instead.
            RunSqlRunner($"UPDATE '{tablePath}' T SET T.'INTVAL' = T.'INTVAL' WHERE T.'SECVAL' = -32000");

            // Round-trip: insert then delete a throwaway row via BDE itself.
            RunSqlRunner($"INSERT INTO '{tablePath}' (SECVAL) VALUES (-1)");
            RunSqlRunner($"DELETE FROM '{tablePath}' T WHERE T.'SECVAL' = -1");
        }

        // --------------------------------------------------------------------
        // Diagnostic mode: exercise a real, user-supplied multi-level .PX
        // table (TESTTAB_LARGEDATA.DB) to validate ParadoxPrimaryKey.Enumerate's
        // non-leaf (indexLevel != 0) traversal against real data.
        // --------------------------------------------------------------------
        //
        // TESTTAB_LARGEDATA.DB schema (per BB's Database Desktop structure info):
        //   Id        N   (numeric, primary key)
        //   Sender    C(255)
        //   Recipient C(255)
        //   Message   C(255)
        //   Timestamp dt
        //   SendId    C(255)
        //   ReplyId   C(255)
        // Indexes: primary key on Id; secondary index ReplyIdIndex on ReplyId.
        //
        // This table is large enough (.PX is ~670KB) that its primary index
        // is expected to have grown past a single block/level, unlike the
        // small TESTTAB.DB fixture used elsewhere in this harness. The user
        // has indicated this is a disposable backup copy, so destructive
        // operations (append/update/delete) are fine to run against it too.
        // --------------------------------------------------------------------
        // Password / encryption reverse-engineering validation.
        //
        // Validates our C# port of pxlib's px_crypt.c against the sample
        // passworded tables (password = "password"):
        //   1. Confirms PxCrypt.PasswordChecksum("password") matches the
        //      encryption key actually stored in testtab_passworded.DB's header.
        //   2. Confirms testtab_passworded_withdata.DB can be transparently
        //      decrypted and enumerated using that key, producing the
        //      expected rows.
        // --------------------------------------------------------------------
        private static void RunPxPasswordTest()
        {
            string dataDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data");
            string emptyPath = Path.Combine(dataDir, "testtab_passworded.DB");
            string dataPath = Path.Combine(dataDir, "testtab_passworded_withdata.DB");

            int checksum = ParadoxReader.PxCrypt.PasswordChecksum("password");
            Console.WriteLine($"PxCrypt.PasswordChecksum(\"password\") = 0x{unchecked((uint)checksum):X8}");

            using (var file = new ParadoxFile(emptyPath))
            {
                Console.WriteLine($"{Path.GetFileName(emptyPath)}: EncryptionKey = 0x{file.EncryptionKey:X8}, IsEncrypted = {file.IsEncrypted}");
                bool match = file.EncryptionKey == unchecked((uint)checksum);
                Console.WriteLine(match ? "MATCH: checksum algorithm confirmed." : "MISMATCH: checksum algorithm needs review.");
            }

            using (var table = new ParadoxTableFile(dataPath))
            {
                Console.WriteLine($"{Path.GetFileName(dataPath)}: EncryptionKey = 0x{table.EncryptionKey:X8}, RecordCount = {table.RecordCount}");
                int rowNum = 0;
                foreach (var rec in table.Enumerate())
                {
                    rowNum++;
                    var values = string.Join(", ", rec.DataValues.Select(v => v?.ToString() ?? "<null>").ToArray());
                    Console.WriteLine($"  Row {rowNum}: {values}");
                }
                Console.WriteLine(rowNum > 0 ? $"Decrypted and enumerated {rowNum} row(s) successfully." : "No rows enumerated - decryption may have failed.");
            }
        }

        private static void RunPxPasswordWriteTest(string dbPath)
        {
            using (var table = new ParadoxTableFile(dbPath))
            {
                Console.WriteLine($"{Path.GetFileName(dbPath)}: EncryptionKey = 0x{table.EncryptionKey:X8}");

                var rec = table.Enumerate().First();
                Console.WriteLine("Before update: " + string.Join(", ", rec.DataValues.Select(v => v?.ToString() ?? "<null>").ToArray()));

                var newValues = (object[])rec.DataValues.Clone();
                newValues[F_INTVAL] = 555555;
                table.UpdateRecord(rec, newValues);
            }

            using (var table = new ParadoxTableFile(dbPath))
            {
                var rec = table.Enumerate().First();
                Console.WriteLine("After update (reopened): " + string.Join(", ", rec.DataValues.Select(v => v?.ToString() ?? "<null>").ToArray()));
                bool ok = Convert.ToInt32(rec.DataValues[F_INTVAL]) == 555555;
                Console.WriteLine(ok ? "WRITE PATH OK: encrypted round-trip verified." : "WRITE PATH FAILED.");

                int rowCount = table.Enumerate().Count();
                Console.WriteLine($"Row count after update: {rowCount}");
            }
        }

        // --------------------------------------------------------------------
        // Diagnostic mode: recreate a fresh equivalent table + composite
        // secondary index via SQLRunner and diff its .XG0 against a corrupt
        // corpus .XG0 file, byte-for-byte, to determine whether observed
        // mismatches (e.g. PracticeCorrespond.XG0's on-disk RecordSize=10 vs
        // the 12 our field-mapping computes) are a code-side bug or genuine
        // data corruption / Paradox-version drift.
        // --------------------------------------------------------------------
        private static void RunXg0DiagMode(string corruptXG0Path)
        {
            const string diagFolder = @"c:\temp\xg0diag";
            const string diagTable  = "XG0DIAGTAB";

            if (!File.Exists(corruptXG0Path))
            {
                Console.WriteLine("[xg0diag] Corrupt reference file not found: {0}", corruptXG0Path);
                return;
            }

            Directory.CreateDirectory(diagFolder);
            foreach (var f in Directory.GetFiles(diagFolder))
            {
                try { File.Delete(f); } catch { /* best effort */ }
            }

            string tablePath = Path.Combine(diagFolder, diagTable + ".DB");

            // Recreate PracticeCorrespond's schema as discovered from the corpus
            // .DB header: Id AUTOINC (PK), PracticeId INTEGER, CorrespondenceMethodId
            // INTEGER, CarrierMethodIdentifier CHAR(255), CarrierOutputPath CHAR(255).
            string createTableSql =
                "CREATE TABLE '" + tablePath + "' (" +
                "Id AUTOINC, " +
                "PracticeId INTEGER, " +
                "CorrespondenceMethodId INTEGER, " +
                "CarrierMethodIdentifier CHARACTER(255), " +
                "CarrierOutputPath CHARACTER(255), " +
                "PRIMARY KEY (Id))";

            string createIndexSql =
                "CREATE INDEX SECIDX ON '" + tablePath + "' (PracticeId, CorrespondenceMethodId)";

            Console.WriteLine("[xg0diag] Creating fresh reference table via SQLRunner...");
            RunDiagSqlRunner(diagFolder, createTableSql);
            RunDiagSqlRunner(diagFolder, createIndexSql);

            if (!File.Exists(tablePath))
            {
                Console.WriteLine("[xg0diag] SQLRunner did not produce {0}; aborting.", tablePath);
                return;
            }

            Console.WriteLine("[xg0diag] Inserting rows via SQLRunner...");
            for (int i = 1; i <= 6; i++)
            {
                string insertSql =
                    "INSERT INTO '" + tablePath + "' " +
                    "(PracticeId, CorrespondenceMethodId, CarrierMethodIdentifier, CarrierOutputPath) VALUES (" +
                    i + ", " + (i % 3) + ", 'method" + i + "', 'path" + i + "')";
                RunDiagSqlRunner(diagFolder, insertSql);
            }

            string freshXG0Path = Path.Combine(diagFolder, diagTable + ".XG0");
            if (!File.Exists(freshXG0Path))
            {
                Console.WriteLine("[xg0diag] Fresh .XG0 was not created at {0}; aborting comparison.", freshXG0Path);
                return;
            }

            Console.WriteLine();
            Console.WriteLine("=== [xg0diag] Fresh reference: {0} ===", freshXG0Path);
            DumpIndexFile(freshXG0Path);

            Console.WriteLine();
            Console.WriteLine("=== [xg0diag] Corrupt corpus file: {0} ===", corruptXG0Path);
            DumpIndexFile(corruptXG0Path);
        }

        /// <summary>
        /// Dumps an .Xnn/.Xgn/.Ynn/.Ygn index file's header fields, own field
        /// definitions, and root-block entries to the console for manual
        /// comparison. Uses the same on-disk offsets ParadoxFile.ReadHeader
        /// and SecondaryIndexFile.ReadBlock rely on.
        /// </summary>
        private static void DumpIndexFile(string path)
        {
            using (var f = new ParadoxFile(path))
            {
                Console.WriteLine("  RecordSize={0}, HeaderSize={1}, FileType={2}, maxTableSize={3}, RecordCount={4}",
                    f.RecordSize, f.headerSize, f.FileType, f.maxTableSize, f.RecordCount);
                Console.WriteLine("  nextBlock={0}, fileBlocks={1}, firstBlock={2}, lastBlock={3}",
                    f.nextBlock, f.fileBlocks, f.firstBlock, f.lastBlock);
                Console.WriteLine("  indexFieldNumber={0}, pxRootBlockId={1}, pxLevelCount={2}, FieldCount={3}, primaryKeyFields={4}",
                    f.indexFieldNumber, f.pxRootBlockId, f.pxLevelCount, f.FieldCount, f.primaryKeyFields);
                Console.WriteLine("  fileVersionID={0}, maxBlocks={1}, changeCount1={2}, changeCount2={3}, autoIncVal={4}",
                    f.fileVersionID, f.maxBlocks, f.changeCount1, f.changeCount2, f.autoIncVal);
                Console.WriteLine("  encryption1={0}, indexUpdateRequired={1}",
                    f.EncryptionKey, f.indexUpdateRequired);

                if (f.FieldTypes != null)
                {
                    for (int i = 0; i < f.FieldTypes.Length; i++)
                    {
                        Console.WriteLine("  field[{0}] type={1} size={2}",
                            i, f.FieldTypes[i].fType, f.FieldTypes[i].fSize);
                    }
                }

                int blockSize = f.maxTableSize * 0x400;
                long rootPos  = f.headerSize + (long)f.pxRootBlockId * blockSize;
                // Try both 0-based and 1-based root block interpretations, same
                // as SecondaryIndexFile.ResolveBlockBase, and print whichever
                // resolves to a valid in-file position.
                long rootPos0 = f.headerSize + (long)f.pxRootBlockId * blockSize;
                long rootPos1 = f.headerSize + (long)(f.pxRootBlockId - 1) * blockSize;
                long chosenPos = (rootPos1 >= f.headerSize && rootPos1 + blockSize <= f.stream.Length) ? rootPos1 : rootPos0;

                if (chosenPos < f.headerSize || chosenPos + blockSize > f.stream.Length)
                {
                    Console.WriteLine("  [root block position out of range: {0}, streamLength={1}]", chosenPos, f.stream.Length);
                    return;
                }

                f.stream.Position = chosenPos;
                var raw = new byte[blockSize];
                int totalRead = 0;
                while (totalRead < blockSize)
                {
                    int n = f.stream.Read(raw, totalRead, blockSize - totalRead);
                    if (n <= 0) break;
                    totalRead += n;
                }

                ushort leftChild = BitConverter.ToUInt16(raw, 0);
                ushort reserved  = BitConverter.ToUInt16(raw, 2);
                ushort usedBytes = BitConverter.ToUInt16(raw, 4);
                Console.WriteLine("  rootBlock @ {0}: leftChild={1}, reserved={2}, usedBytes={3}",
                    chosenPos, leftChild, reserved, usedBytes);

                int keyDataSize = 0;
                if (f.FieldTypes != null)
                    foreach (var ft in f.FieldTypes) keyDataSize += ft.fSize;

                int pointerSize = f.RecordSize > keyDataSize ? f.RecordSize - keyDataSize : 6;
                int entrySize   = keyDataSize + pointerSize;
                Console.WriteLine("  derived: keyDataSize={0}, pointerSize={1}, entrySize={2}", keyDataSize, pointerSize, entrySize);

                int entryCount = usedBytes == 0 ? 1 : (usedBytes / entrySize) + 1;
                int pos = 6;
                for (int i = 0; i < entryCount && pos + entrySize <= blockSize; i++)
                {
                    var keyBytes = new byte[keyDataSize];
                    Array.Copy(raw, pos, keyBytes, 0, keyDataSize);
                    string keyHex = BitConverter.ToString(keyBytes);

                    ushort bnEnc = (ushort)((raw[pos + keyDataSize] << 8) | raw[pos + keyDataSize + 1]);
                    ushort bn    = (ushort)(bnEnc ^ 0x8000);

                    ushort rc = 0;
                    if (pointerSize >= 6)
                    {
                        ushort rcEnc = (ushort)((raw[pos + keyDataSize + 2] << 8) | raw[pos + keyDataSize + 3]);
                        rc = (ushort)(rcEnc ^ 0x8000);
                    }

                    Console.WriteLine("  entry[{0}] key={1} bn={2} rc={3}", i, keyHex, bn, rc);
                    pos += entrySize;
                }
            }
        }

        /// <summary>
        /// Minimal, self-contained SQLRunner invocation for xg0diag mode
        /// (mirrors CorpusTest.RunSqlRunner).
        /// </summary>
        private static void RunDiagSqlRunner(string workDir, string sql)
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
                // Drain stdout/stderr asynchronously - SQLRunner can write enough
                // output to fill the redirected pipe buffer, which would otherwise
                // deadlock the process (and cause WaitForExit to time out and get
                // killed before it finishes/flushes the operation to disk).
                process.OutputDataReceived += (s, e) => { };
                process.ErrorDataReceived  += (s, e) => { };
                process.Start();
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();

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

        // --------------------------------------------------------------------
        // Diagnostic mode: grow the .PX primary index past a single level
        // --------------------------------------------------------------------
        //
        // Bulk-appends records via ParadoxReader (fast, no BDE round trip)
        // until the .PX file's pxLevelCount (exposed via the new
        // ParadoxPrimaryKey.LevelCount/RootBlockId test-only properties)
        // exceeds 0, i.e. the primary index root block has split and the
        // tree now has an internal (non-leaf) level. This lets us observe
        // whether ParadoxPrimaryKey.Enumerate's "else" (indexLevel != 0)
        // branch is reachable/correct on a real multi-level index, before
        // deciding whether any fix is needed.
        private static void RunGrowPxIndexMode(int targetCount)
        {
            ResetTestFolder();

            int lastReportedLevel = -1;
            int appended = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                for (int i = 1; i <= targetCount; i++)
                {
                    var values = MakeSampleFieldValues((short)(i % 30000), "grow" + i);
                    table.AppendRecord(values);
                    appended++;

                    var level = table.PrimaryKeyIndex?.LevelCount ?? -1;
                    if (level != lastReportedLevel)
                    {
                        Console.WriteLine(
                            "  [growpxindex] after {0} appended records ({1:0.0}s elapsed): pxLevelCount={2}, pxRootBlockId={3}",
                            appended, sw.Elapsed.TotalSeconds, level, table.PrimaryKeyIndex?.RootBlockId);
                        lastReportedLevel = level;
                    }
                    else if (appended % 1000 == 0)
                    {
                        Console.WriteLine(
                            "  [growpxindex] progress: {0} appended ({1:0.0}s elapsed), pxLevelCount={2}",
                            appended, sw.Elapsed.TotalSeconds, level);
                    }
                }

                Console.WriteLine(
                    "[growpxindex] Done. Appended {0} records; final pxLevelCount={1}, pxRootBlockId={2}",
                    appended, table.PrimaryKeyIndex?.LevelCount, table.PrimaryKeyIndex?.RootBlockId);
            }

            if (GetLastPxLevel() > 0)
            {
                Console.WriteLine();
                Console.WriteLine("=== [growpxindex] Verifying with SQLRunner (Pdxrbld-equivalent) ===");
                TestVerifyWithSqlRunner();

                Console.WriteLine();
                Console.WriteLine("=== [growpxindex] Condition + primary index lookup smoke test ===");
                TestConditionIndexLookup();
            }
            else
            {
                Console.WriteLine(
                    "[growpxindex] pxLevelCount never exceeded 0 after {0} records; " +
                    "increase targetCount (pass as second arg) and re-run.", appended);
            }
        }

        // Small helper so the post-loop checks above can re-open the table
        // read-only to confirm the on-disk pxLevelCount after the using
        // block above has closed/flushed it.
        private static int GetLastPxLevel()
        {
            using (var table = new ParadoxTableFile(TestFolder, TestTabTableName))
            {
                return table.PrimaryKeyIndex?.LevelCount ?? -1;
            }
        }

        private static void RunSqlRunner(string sql)
        {
            Console.WriteLine("SQLRunner> {0}", sql);

            // Guarantee a clean starting state before every single invocation:
            // kill any lingering SQLRunner process (a prior call may have
            // hung and been force-killed, or a stray instance may still be
            // shutting down) and remove all *.LCK files, then verify both
            // are actually gone before we start a new process. Without this,
            // multiple SQLRunner instances can pile up concurrently and
            // fight over the same locks, which is its own source of hangs.
            EnsureNoSqlRunnerProcessesRunning();
            DeleteStaleLockFilesWithRetry();

            var remainingLocks = Directory.GetFiles(TestFolder, "*.LCK");
            if (remainingLocks.Length > 0)
            {
                Console.WriteLine("  [warn] {0} lock file(s) still present before launch: {1}",
                    remainingLocks.Length, string.Join(", ", remainingLocks.Select(Path.GetFileName).ToArray()));
            }

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

            var stdoutBuilder = new System.Text.StringBuilder();
            var stderrBuilder = new System.Text.StringBuilder();

            using (var process = new Process { StartInfo = psi, EnableRaisingEvents = true })
            {
                process.OutputDataReceived += (s, e) => { if (e.Data != null) stdoutBuilder.AppendLine(e.Data); };
                process.ErrorDataReceived  += (s, e) => { if (e.Data != null) stderrBuilder.AppendLine(e.Data); };

                process.Start();
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();

                // SQLRunner's /S flag appears to suppress the confirmation
                // prompt only for INSERT; UPDATE/DELETE still print
                // "Please ensure you have a Backup." and then wait on stdin
                // for an ENTER keypress before proceeding, which our
                // redirected (non-interactive) stdin never provides. Feed a
                // single newline proactively so it doesn't block waiting for
                // input that will never come.
                try
                {
                    process.StandardInput.WriteLine();
                    process.StandardInput.Flush();
                }
                catch { /* process may have already exited or not be waiting on stdin */ }

                // Per user instruction: treat SQLRunner as locked-up if it
                // hasn't exited within ~10 seconds, rather than waiting 30s.
                // This is our hang detector - a real "problem state", not a
                // slow-but-working call.
                if (!process.WaitForExit(10000))
                {
                    Console.WriteLine("  [warn] SQLRunner did not exit within 10s; treating as HUNG. Killing process.");
                    KillProcessTree(process);
                    process.WaitForExit();
                }

                string stdout = stdoutBuilder.ToString();
                string stderr = stderrBuilder.ToString();

                if (!Net35Compat.IsNullOrWhiteSpace(stdout))
                    Console.WriteLine(stdout.Trim());
                if (!Net35Compat.IsNullOrWhiteSpace(stderr))
                    Console.WriteLine("  [stderr] " + stderr.Trim());
            }

            // BDE can hold onto its file handles / PDOXUSRS.LCK briefly after
            // the SQLRunner process itself has exited (e.g. while its BDE
            // session/engine shuts down). Give it a moment before we attempt
            // to touch the table or its lock files again, otherwise a
            // following ParadoxTableFile open (or another SQLRunner call)
            // can race BDE's own cleanup and see a "table is busy" state.
            System.Threading.Thread.Sleep(500);

            // Clean up any lock file SQLRunner itself left behind, so the next
            // invocation (or a subsequent ParadoxTableFile open) doesn't see a
            // stale lock. Retry with backoff since BDE may still be releasing
            // the handle for a short time after the process exits.
            DeleteStaleLockFilesWithRetry();
        }

        /// <summary>
        /// Deletes any *.LCK files in the test folder, retrying briefly if a
        /// file is still in use (BDE can hold the handle open for a short
        /// time after SQLRunner's process has exited).
        /// </summary>
        private static void DeleteStaleLockFilesWithRetry(int maxAttempts = 5, int delayMs = 250)
        {
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                var remaining = Directory.GetFiles(TestFolder, "*.LCK");
                if (remaining.Length == 0) return;

                bool anyFailed = false;
                foreach (var lockFile in remaining)
                {
                    try { File.Delete(lockFile); }
                    catch (Exception ex)
                    {
                        anyFailed = true;
                        if (attempt == maxAttempts)
                            Console.WriteLine("  [warn] Could not delete {0} after {1} attempts: {2}", lockFile, maxAttempts, ex.Message);
                    }
                }

                if (!anyFailed) return;
                System.Threading.Thread.Sleep(delayMs);
            }
        }


        /// <summary>
        /// Deletes any *.LCK files in the test folder. Safe to call before/after
        /// SQLRunner runs, provided no SQLRunner (or ParadoxTableFile) process
        /// is currently using the table concurrently.
        /// </summary>
        private static void DeleteStaleLockFiles()
        {
            foreach (var lockFile in Directory.GetFiles(TestFolder, "*.LCK"))
            {
                try { File.Delete(lockFile); }
                catch (Exception ex) { Console.WriteLine("  [warn] Could not delete {0}: {1}", lockFile, ex.Message); }
            }
        }

        /// <summary>
        /// Guarantees no SQLRunner process is left running before we launch a
        /// new one. A previous invocation that hung and was force-killed can,
        /// in rare cases, leave a still-shutting-down instance behind (or a
        /// completely separate stray instance from an earlier crashed run of
        /// this harness). Running multiple SQLRunner/BDE instances
        /// concurrently against the same table is itself a reliable way to
        /// cause locking problems, so this must be checked/cleared before
        /// every single RunSqlRunner call, not just once at startup.
        /// </summary>
        private static void EnsureNoSqlRunnerProcessesRunning()
        {
            var exeName = Path.GetFileNameWithoutExtension(SqlRunnerExePath);
            var stray = Process.GetProcessesByName(exeName);
            if (stray.Length == 0) return;

            Console.WriteLine("  [warn] {0} stray SQLRunner process(es) found before launch (PIDs: {1}); killing.",
                stray.Length, string.Join(", ", stray.Select(p => p.Id.ToString()).ToArray()));

            foreach (var p in stray)
            {
                try { KillProcessTree(p); }
                catch (Exception ex) { Console.WriteLine("  [warn] Failed to kill PID {0}: {1}", p.Id, ex.Message); }
                finally { p.Dispose(); }
            }

            // Give the OS a moment to fully tear down the killed process(es)
            // before we go on to check/delete lock files.
            System.Threading.Thread.Sleep(500);

            var stillRunning = Process.GetProcessesByName(exeName);
            if (stillRunning.Length > 0)
            {
                Console.WriteLine("  [warn] {0} SQLRunner process(es) still running after kill attempt (PIDs: {1}).",
                    stillRunning.Length, string.Join(", ", stillRunning.Select(p => p.Id.ToString()).ToArray()));
                foreach (var p in stillRunning) p.Dispose();
            }
        }

        /// <summary>
        /// Kills a process and any child processes it may have spawned
        /// (taskkill /T), rather than relying on Process.Kill() alone, which
        /// only kills the immediate process and can leave children running.
        /// </summary>
        private static void KillProcessTree(Process process)
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
            catch { /* fall back to direct kill below */ }

            try { if (!process.HasExited) process.Kill(); } catch { /* best effort */ }
        }
    }
}
