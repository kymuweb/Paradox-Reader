using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Focused regression harness for the memo-value-set path specifically
    /// (as opposed to <see cref="CreateTableCompareTest"/>, which only ever
    /// creates + does a single blank-memo insert).
    ///
    /// Uses the same schema shape as HeaderCompareTest's PKALPMEMO case (ID
    /// Long PK + NAME Alpha(20) + NOTES MemoBLOb with a 20-byte inline leader,
    /// i.e. fSize=30), and drives BOTH "ours" (ParadoxReader) and "sqlrunner"
    /// (real BDE) through the exact same 5-stage sequence:
    ///
    ///   1. CREATE TABLE (0 rows)
    ///   2. INSERT one row with NAME set but NOTES left blank/null
    ///   3. UPDATE NOTES to a 19-char value (fits inline: leader=20, 1 byte to spare)
    ///   4. UPDATE NOTES to a 20-char value (fits inline exactly: leader=20)
    ///   5. UPDATE NOTES to a 21-char value (does NOT fit inline, must externalize to .MB)
    ///
    /// A full-file byte snapshot of every table file (.DB/.PX/.MB) is taken
    /// after each stage for both sides, and each stage's snapshot is
    /// byte-compared between "ours" and "sqlrunner" to spot divergence as
    /// early as possible (rather than only at the very end).
    ///
    /// This is purely diagnostic (prints reports); it does not fail the
    /// process on a diff.
    /// </summary>
    internal static class CreateTableMemoTest
    {
        private const string RootDir = @"c:\temp\createtablememotest";
        private const string BaseName = "PKALPMEM";

        private static readonly (string Label, string Memo)[] UpdateStages =
        {
            ("19chars", new string('a', 19)),
            ("20chars", new string('b', 20)),
            ("21chars", new string('c', 21)),
        };

        public static void Run()
        {
            if (!SqlRunner.IsAvailable)
            {
                Console.WriteLine("[createtablememotest] SQLRunner not found at {0}; aborting.", SqlRunner.ExePath);
                return;
            }

            Directory.CreateDirectory(RootDir);

            string caseDir = Path.Combine(RootDir, "PKALPMEMO");
            string oursDir = Path.Combine(caseDir, "ours");
            string sqlDir = Path.Combine(caseDir, "sqlrunner");
            ResetDir(oursDir);
            ResetDir(sqlDir);

            string oursDbPath = Path.Combine(oursDir, BaseName + ".DB");
            string sqlDbPath = Path.Combine(sqlDir, BaseName + ".DB");

            var schema = new TableSchemaDefinition
            {
                TableName = BaseName,
                Fields =
                {
                    new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
                    new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                    // Leader=20 inline bytes (fSize=30 on disk), matching SQLRunner's
                    // BLOB(20,1) DDL below.
                    TableFieldDefinition.CreateMemoField("NOTES", ParadoxFieldTypes.MemoBLOb, 20),
                }
            };

            var snapshots = new List<string>();

            // ---------------------------------------------------------
            // Stage 1: CREATE TABLE (0 rows)
            // ---------------------------------------------------------
            Console.WriteLine();
            Console.WriteLine("======================================================================");
            Console.WriteLine("Stage 1: CREATE TABLE (0 rows)");
            Console.WriteLine("======================================================================");

            TableCreator.CreateNew(oursDbPath, schema);
            RunSqlRunner("CREATE TABLE '" + sqlDbPath + "' (ID INTEGER, NAME CHARACTER(20), NOTES BLOB(20,1), PRIMARY KEY (ID))", sqlDbPath);

            if (!File.Exists(sqlDbPath))
            {
                Console.WriteLine("  [FAIL] SQLRunner did not create {0}; aborting.", sqlDbPath);
                return;
            }

            Snapshot(oursDir, sqlDir, "1_create_0rows");
            CompareStage("1_create_0rows", oursDir, sqlDir);

            // ---------------------------------------------------------
            // Stage 2: INSERT one row, NAME set, NOTES blank/null
            // ---------------------------------------------------------
            Console.WriteLine();
            Console.WriteLine("======================================================================");
            Console.WriteLine("Stage 2: INSERT one row (NAME=abc, NOTES=blank)");
            Console.WriteLine("======================================================================");

            TryInsert(oursDbPath, new object[] { 1, "abc", null }, "ours");
            RunSqlRunner("INSERT INTO '" + sqlDbPath + "' (ID, NAME) VALUES (1, 'abc')", sqlDbPath);

            Snapshot(oursDir, sqlDir, "2_insert_blankMemo");
            CompareStage("2_insert_blankMemo", oursDir, sqlDir);

            // ---------------------------------------------------------
            // Stages 3-5: UPDATE NOTES to 19/20/21-char values
            // ---------------------------------------------------------
            foreach (var stage in UpdateStages)
            {
                Console.WriteLine();
                Console.WriteLine("======================================================================");
                Console.WriteLine("Stage: UPDATE NOTES to {0} value", stage.Label);
                Console.WriteLine("======================================================================");

                TryUpdateMemo(oursDbPath, 1, stage.Memo, "ours");
                RunSqlRunner("UPDATE '" + sqlDbPath + "' SET '" + sqlDbPath + "'.'NOTES' = '" + stage.Memo + "' WHERE '" + sqlDbPath + "'.'id' = 1", sqlDbPath);

                string label = "3_update_" + stage.Label;
                Snapshot(oursDir, sqlDir, label);
                CompareStage(label, oursDir, sqlDir);
            }

            Console.WriteLine();
            Console.WriteLine("======================================================================");
            Console.WriteLine("Done. Snapshots captured under:");
            Console.WriteLine("  ours:      {0}\\snapshots\\<stage>", oursDir);
            Console.WriteLine("  sqlrunner: {0}\\snapshots\\<stage>", sqlDir);
            Console.WriteLine("======================================================================");
        }

        private static bool TryInsert(string dbPath, object[] fieldValues, string label)
        {
            try
            {
                using (var table = new ParadoxTableFile(dbPath))
                    table.InsertRecord(fieldValues);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] InsertRecord into {0} table threw: {1}", label, ex.Message);
                return false;
            }
        }

        private static bool TryUpdateMemo(string dbPath, int id, string memoText, string label)
        {
            try
            {
                using (var table = new ParadoxTableFile(dbPath))
                {
                    foreach (var rec in table.Enumerate())
                    {
                        if (Convert.ToInt32(rec.DataValues[0]) != id)
                            continue;

                        var newValues = new object[rec.DataValues.Length];
                        Array.Copy(rec.DataValues, newValues, rec.DataValues.Length);

                        var origMemo = newValues[2] as MemoValue;
                        newValues[2] = new MemoValue(memoText, origMemo?.BlobInfo);

                        table.UpdateRecord(rec, newValues);
                        return true;
                    }
                }
                Console.WriteLine("  [FAIL] {0}: record id={1} not found for memo update.", label, id);
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] UpdateRecord (memo) on {0} table threw: {1}", label, ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Copies the current .DB/.PX/.MB files for both "ours" and
        /// "sqlrunner" into per-stage snapshot subfolders, so every stage's
        /// on-disk state is preserved for later inspection even though later
        /// stages overwrite the live files.
        /// </summary>
        private static void Snapshot(string oursDir, string sqlDir, string stageLabel)
        {
            CopySnapshot(oursDir, stageLabel);
            CopySnapshot(sqlDir, stageLabel);
        }

        private static void CopySnapshot(string dir, string stageLabel)
        {
            string snapDir = Path.Combine(dir, "snapshots", stageLabel);
            Directory.CreateDirectory(snapDir);
            foreach (var ext in new[] { ".DB", ".PX", ".MB" })
            {
                string src = Path.Combine(dir, BaseName + ext);
                if (File.Exists(src))
                    File.Copy(src, Path.Combine(snapDir, BaseName + ext), overwrite: true);
            }
        }

        /// <summary>
        /// Byte-compares the just-captured snapshot for this stage between
        /// "ours" and "sqlrunner", printing any divergence immediately (so
        /// each stage's report appears right after that stage runs, rather
        /// than only in one combined report at the very end).
        /// </summary>
        private static void CompareStage(string stageLabel, string oursDir, string sqlDir)
        {
            string oursSnapDir = Path.Combine(oursDir, "snapshots", stageLabel);
            string sqlSnapDir = Path.Combine(sqlDir, "snapshots", stageLabel);

            Console.WriteLine("--- Byte comparison after stage '{0}' ---", stageLabel);

            var oursFiles = Directory.GetFiles(oursSnapDir)
                .ToDictionary(f => Path.GetExtension(f).ToUpperInvariant(), f => f);
            var sqlFiles = Directory.GetFiles(sqlSnapDir)
                .ToDictionary(f => Path.GetExtension(f).ToUpperInvariant(), f => f);

            var allExtensions = oursFiles.Keys.Union(sqlFiles.Keys).OrderBy(e => e).ToList();
            int totalDiffs = 0;

            foreach (var ext in allExtensions)
            {
                if (!oursFiles.ContainsKey(ext))
                {
                    Console.WriteLine("  [{0}] MISSING on ours side", ext);
                    totalDiffs++;
                    continue;
                }
                if (!sqlFiles.ContainsKey(ext))
                {
                    Console.WriteLine("  [{0}] MISSING on sqlrunner side", ext);
                    totalDiffs++;
                    continue;
                }

                totalDiffs += CompareFilePair(ext, oursFiles[ext], sqlFiles[ext]);
            }

            if (totalDiffs == 0)
                Console.WriteLine("  All paired files byte-identical.");
        }

        private static int CompareFilePair(string ext, string oursPath, string sqlPath)
        {
            byte[] oursBytes = File.ReadAllBytes(oursPath);
            byte[] sqlBytes = File.ReadAllBytes(sqlPath);

            int minLen = Math.Min(oursBytes.Length, sqlBytes.Length);
            int maxLen = Math.Max(oursBytes.Length, sqlBytes.Length);
            int diffCount = 0;

            if (oursBytes.Length != sqlBytes.Length)
            {
                Console.WriteLine("  [{0}] File size differs: ours={1} sqlrunner={2}", ext, oursBytes.Length, sqlBytes.Length);
                diffCount++;
            }

            const int maxOffsetsToPrint = 40;
            int printed = 0;
            for (int i = 0; i < minLen; i++)
            {
                if (oursBytes[i] != sqlBytes[i])
                {
                    if (printed < maxOffsetsToPrint)
                    {
                        Console.WriteLine("  [{0}] offset 0x{1:X4}: ours=0x{2:X2} sqlrunner=0x{3:X2}",
                            ext, i, oursBytes[i], sqlBytes[i]);
                        printed++;
                    }
                    else if (printed == maxOffsetsToPrint)
                    {
                        Console.WriteLine("  [{0}] ...additional differing offsets suppressed...", ext);
                        printed++;
                    }
                    diffCount++;
                }
            }

            if (maxLen > minLen)
            {
                Console.WriteLine("  [{0}] {1} extra trailing byte(s) beyond the shorter file not compared.", ext, maxLen - minLen);
            }

            return diffCount;
        }

        private static void ResetDir(string dir)
        {
            if (Directory.Exists(dir))
            {
                foreach (var f in Directory.GetFiles(dir, "*", SearchOption.AllDirectories))
                {
                    try { File.Delete(f); } catch { /* best effort */ }
                }
                foreach (var d in Directory.GetDirectories(dir, "*", SearchOption.AllDirectories).OrderByDescending(x => x.Length))
                {
                    try { Directory.Delete(d); } catch { /* best effort */ }
                }
            }
            else
            {
                Directory.CreateDirectory(dir);
            }
        }

        private static void RunSqlRunner(string sql, string lockFileFolder) =>
            SqlRunner.Execute(sql, Path.GetDirectoryName(lockFileFolder));
    }
}
