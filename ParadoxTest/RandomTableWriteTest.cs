using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Picks random .db tables from the legacy WriteRows database folder, discovers their schema through
    /// ParadoxReader, and for each one performs many SINGLE writes (more than 256 by default, so new blocks,
    /// index splits and write-counter wrap-around are all exercised), validating after every write.
    ///
    /// Per table, two passes (original table set restored in between and at the end):
    ///   A. legacy only:   legacy writes row i                                  -> validate
    ///   B. interleaved:   ParadoxReader writes row i -> validate -> legacy writes probe row i -> validate
    /// In B the legacy append is the "index out of date" probe: if ParadoxReader left an index the legacy
    /// code considers stale, the legacy write (or the next ParadoxReader open) fails and is reported.
    /// Validation = table opens, header RecordCount grew by exactly the rows written, and (every 25 writes and at
    /// the end) a full enumeration returns RecordCount rows.
    /// </summary>
    internal static class RandomTableWriteTest
    {
        private class ColumnInfo
        {
            public string Name;
            public ParadoxFieldTypes Type;
            public int Size;
        }

        private static int ProbeEvery = 1;
        private static bool LegacyOnly = false;

        public static void Run(int tableCount = 12, int writesPerTable = 260, int seed = 0, string folder = null, string onlyTable = null, int probeEvery = 1)
        {
            LegacyOnly = probeEvery == 0;
            ProbeEvery = Math.Max(1, probeEvery);
            folder = folder ?? LegacyWriteRows.LegacyWriteRowsDatabaseFolderPath;
            if (seed == 0) seed = Environment.TickCount;
            var rng = new Random(seed);

            var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var tempRoot = Path.Combine(@"C:\temp", "paradoxrandomtest_" + stamp);
            Directory.CreateDirectory(tempRoot);
            var report = new StringBuilder();
            Log(report, $"folder={folder} tables={tableCount} writesPerTable={writesPerTable} seed={seed}");

            var candidates = Directory.GetFiles(folder, "*.db")
                .Where(p => Path.GetFileNameWithoutExtension(p).IndexOf(".bak", StringComparison.OrdinalIgnoreCase) < 0
                         && Path.GetFileNameWithoutExtension(p).IndexOf("_legacy_", StringComparison.OrdinalIgnoreCase) < 0
                         && Path.GetFileNameWithoutExtension(p).IndexOf("_paradoxreader_", StringComparison.OrdinalIgnoreCase) < 0
                         && new FileInfo(p).Length < 8 * 1024 * 1024)
                .Where(p => onlyTable == null || string.Equals(Path.GetFileNameWithoutExtension(p), onlyTable, StringComparison.OrdinalIgnoreCase))
                .OrderBy(p => rng.Next())
                .ToList();
            Log(report, $"{candidates.Count} candidate tables");

            int tested = 0, passed = 0, failed = 0, skipped = 0;
            foreach (var path in candidates)
            {
                if (tested >= tableCount) break;
                var tableName = Path.GetFileNameWithoutExtension(path);

                List<ColumnInfo> cols;
                string skipReason = TryGetColumns(path, out cols);
                if (skipReason != null)
                {
                    skipped++;
                    Log(report, $"SKIP {tableName}: {skipReason}");
                    continue;
                }

                tested++;
                bool ok = TestTable(folder, tableName, cols, writesPerTable, rng, tempRoot, report);
                if (ok) passed++; else failed++;
            }

            Log(report, $"SUMMARY tested={tested} passed={passed} failed={failed} skipped={skipped} seed={seed}");
            var reportPath = Path.Combine(tempRoot, "randomtest_" + stamp + ".txt");
            File.WriteAllText(reportPath, report.ToString());
            Console.WriteLine("[randomtest] report: " + reportPath);
        }

        private static string TryGetColumns(string path, out List<ColumnInfo> cols)
        {
            cols = new List<ColumnInfo>();
            try
            {
                using (var t = new ParadoxTableFile(path))
                {
                    for (int i = 0; i < t.FieldNames.Length; i++)
                    {
                        var ft = t.FieldTypes[i];
                        switch (ft.fType)
                        {
                            case ParadoxFieldTypes.Alpha:
                            case ParadoxFieldTypes.Short:
                            case ParadoxFieldTypes.Long:
                            case ParadoxFieldTypes.AutoInc:
                            case ParadoxFieldTypes.Date:
                            case ParadoxFieldTypes.Timestamp:
                            case ParadoxFieldTypes.Number:
                            case ParadoxFieldTypes.Currency:
                            case ParadoxFieldTypes.Logical:
                                break;
                            default:
                                return "unsupported field type " + ft.fType + " (" + t.FieldNames[i] + ")";
                        }
                        cols.Add(new ColumnInfo { Name = t.FieldNames[i], Type = ft.fType, Size = ft.fSize });
                    }
                }
                return cols.Count == 0 ? "no fields" : null;
            }
            catch (Exception ex)
            {
                return "cannot open with ParadoxReader: " + ex.Message;
            }
        }

        private static bool TestTable(string folder, string tableName, List<ColumnInfo> cols, int writes, Random rng, string tempRoot, StringBuilder report)
        {
            var backupFolder = Path.Combine(tempRoot, "backup_" + tableName);
            var backedUp = BinaryComparisonTest.CopyTableSet(folder, tableName, backupFolder, tableName);
            if (backedUp.Count == 0)
            {
                Log(report, $"SKIP {tableName}: nothing to back up");
                return true;
            }

            var dbPath = Path.Combine(folder, tableName + ".db");
            var legacyTable = tableName + ".db";
            bool ok = true;
            Log(report, $"=== {tableName}: {cols.Count} fields [{string.Join(", ", cols.Select(c => c.Name + ":" + c.Type))}] files={string.Join(",", backedUp.Select(Path.GetExtension))}");

            try
            {
                int startCount = ReadCount(dbPath);

                // ---- Preflight: can the legacy tool write this table at all? (tool limitation, not a reader fault) ----
                try
                {
                    LegacyWriteRows.WriteRows(new[] { (legacyTable, BuildRow(cols, rng)) });
                }
                catch (Exception ex)
                {
                    Restore(backupFolder, folder, tableName);
                    Log(report, $"  SKIP (legacy tool cannot write this table even on the untouched original): {ex.Message}");
                    return true;
                }
                Restore(backupFolder, folder, tableName);

                bool sqlUsable = true;
                try { SqlInsert(dbPath, cols, BuildRow(cols, rng)); }
                catch (Exception ex)
                {
                    sqlUsable = false;
                    Log(report, $"  NOTE: SQLRunner cannot write the untouched original, SQL probe disabled: {ex.Message}");
                }
                Restore(backupFolder, folder, tableName);

                // ---- Pass A: legacy single writes, validated after each ----
                int step = 0;
                if (ProbeEvery > 1)
                {
                    Log(report, "  A legacy-only pass skipped (quick mode)");
                }
                else
                try
                {
                    for (step = 1; step <= writes; step++)
                    {
                        LegacyWriteRows.WriteRows(new[] { (legacyTable, BuildRow(cols, rng)) });
                        Validate(dbPath, startCount + step, step, writes);
                        StepSnap(folder, tableName, tempRoot, step, "A");
                    }
                    Log(report, $"  A legacy-only x{writes}: OK");
                }
                catch (Exception ex)
                {
                    ok = false;
                    Log(report, $"  A legacy-only FAILED at write {step}: {Classify(ex)}");
                    SaveFailureSnapshot(folder, tableName, tempRoot, "A_legacyonly_write" + step, report);
                }

                Restore(backupFolder, folder, tableName);

                if (LegacyOnly) return ok;

                // ---- Pass B
                step = 0;
                string phase = "";
                int expected = startCount;
                try
                {
                    for (step = 1; step <= writes; step++)
                    {
                        var row = BuildRow(cols, rng);
                        phase = "paradoxreader append";
                        using (var t = new ParadoxTableFile(dbPath))
                            t.AppendRecord(cols.Select(c => row[c.Name]).ToArray());
                        expected++;
                        Validate(dbPath, expected, step, writes);
                        StepSnap(folder, tableName, tempRoot, step, "P");

                        if (step % ProbeEvery != 0 && step != writes) continue;
                        phase = "legacy probe append after paradoxreader write";
                        LegacyWriteRows.WriteRows(new[] { (legacyTable, BuildRow(cols, rng)) });
                        expected++;
                        Validate(dbPath, expected, step, writes);
                        StepSnap(folder, tableName, tempRoot, step, "L");

                        if (sqlUsable)
                        {
                            phase = "sqlrunner probe append after paradoxreader+legacy write";
                            SqlInsert(dbPath, cols, BuildRow(cols, rng));
                            expected++;
                            StepSnap(folder, tableName, tempRoot, step, "S");
                        }

                        phase = "paradoxreader full re-read after probes";
                        Validate(dbPath, expected, 0, writes);
                    }
                    Log(report, $"  B interleaved reader+legacy{(sqlUsable ? "+sqlrunner" : "")} x{writes}: OK (final rows {expected})");
                }
                catch (Exception ex)
                {
                    ok = false;
                    Log(report, $"  B interleaved FAILED at write {step} ({phase}): {Classify(ex)}");
                    SaveFailureSnapshot(folder, tableName, tempRoot, "B_interleaved_write" + step, report);
                }
            }
            finally
            {
                Restore(backupFolder, folder, tableName);
            }
            Console.WriteLine($"[randomtest] {tableName}: {(ok ? "OK" : "FAILED")}");
            return ok;
        }

        private static void StepSnap(string folder, string tableName, string tempRoot, int step, string kind)
        {
            if (Environment.GetEnvironmentVariable("PX_STEPS") != "1") return;
            var dest = Path.Combine(tempRoot, "steps_" + tableName, step.ToString("D4") + kind);
            Directory.CreateDirectory(dest);
            BinaryComparisonTest.CopyTableSet(folder, tableName, dest, tableName);
        }

        private static void SaveFailureSnapshot(string folder, string tableName, string tempRoot, string label, StringBuilder report)
        {
            try
            {
                var dest = Path.Combine(tempRoot, "FAILED_" + tableName + "_" + label);
                var files = BinaryComparisonTest.CopyTableSet(folder, tableName, dest, tableName);
                foreach (var f in new[] { LegacyWriteRows.FailedInputsJsonFilePath, LegacyWriteRows.FailedDetailsLogFilePath })
                    if (File.Exists(f)) File.Copy(f, Path.Combine(dest, Path.GetFileName(f)), true);
                var tmp = Path.GetTempPath();
                foreach (var n in new[] { "writeRowInput.json", "writeRowOutput.json" })
                    if (File.Exists(Path.Combine(tmp, n))) File.Copy(Path.Combine(tmp, n), Path.Combine(dest, "last_" + n), true);
                Log(report, $"  FAILURE SNAPSHOT ({files.Count} table files + json) saved to: {dest}");
            }
            catch (Exception ex)
            {
                Log(report, "  failed to save failure snapshot: " + ex.Message);
            }
        }

        private static string Classify(Exception ex)
        {
            var m = ex.Message ?? string.Empty;
            if (m.IndexOf("OpenTable", StringComparison.OrdinalIgnoreCase) >= 0)
                return "*** OpenTableException - INDEX LIKELY BROKEN BY PREVIOUS WRITE *** " + m;
            return m;
        }

        private static int ReadCount(string dbPath)
        {
            using (var t = new ParadoxTableFile(dbPath))
                return t.RecordCount;
        }

        private static void Validate(string dbPath, int expectedCount, int step, int total)
        {
            using (var t = new ParadoxTableFile(dbPath))
            {
                if (t.RecordCount != expectedCount)
                    throw new Exception($"RecordCount {t.RecordCount} != expected {expectedCount}");
                if (step == 0 || step % 25 == 0 || step == total)
                {
                    int n = t.Enumerate().Count();
                    if (n != expectedCount)
                        throw new Exception($"enumerated {n} rows != expected {expectedCount}");
                }
            }
        }

        private static Dictionary<string, object> BuildRow(List<ColumnInfo> cols, Random rng)
        {
            var row = new Dictionary<string, object>();
            foreach (var c in cols)
            {
                switch (c.Type)
                {
                    case ParadoxFieldTypes.AutoInc: row[c.Name] = 0; break;
                    case ParadoxFieldTypes.Short: row[c.Name] = rng.Next(-30000, 30000); break;
                    case ParadoxFieldTypes.Long: row[c.Name] = rng.Next(1, 100000000); break;
                    case ParadoxFieldTypes.Number:
                    case ParadoxFieldTypes.Currency: row[c.Name] = Math.Round(rng.NextDouble() * 100000, 2); break;
                    case ParadoxFieldTypes.Logical: row[c.Name] = rng.Next(2) == 1; break;
                    case ParadoxFieldTypes.Date: row[c.Name] = new DateTime(1990, 1, 1).AddDays(rng.Next(0, 12000)); break;
                    case ParadoxFieldTypes.Timestamp: row[c.Name] = new DateTime(2000, 1, 1).AddSeconds(rng.Next(0, int.MaxValue / 2)); break;
                    default:
                        // Long values so unique, case-insensitive secondary indexes (e.g. TableLastModified.TableNameIndex) never collide.
                        int len = c.Size >= 8 ? rng.Next(8, Math.Min(c.Size, 24) + 1) : Math.Max(1, rng.Next(1, c.Size + 1));
                        var sb = new StringBuilder(len);
                        for (int i = 0; i < len; i++) sb.Append((char)('A' + rng.Next(26)));
                        row[c.Name] = sb.ToString();
                        break;
                }
            }
            return row;
        }

        private static void SqlInsert(string dbPath, List<ColumnInfo> cols, Dictionary<string, object> row)
        {
            var names = new List<string>();
            var lits = new List<string>();
            foreach (var c in cols)
            {
                object v = row[c.Name];
                string lit;
                switch (c.Type)
                {
                    case ParadoxFieldTypes.AutoInc: continue;
                    case ParadoxFieldTypes.Alpha: lit = "'" + ((string)v).Replace("'", "''") + "'"; break;
                    case ParadoxFieldTypes.Logical: lit = (bool)v ? "TRUE" : "FALSE"; break;
                    case ParadoxFieldTypes.Date: lit = "'" + ((DateTime)v).ToString("MM/dd/yyyy", System.Globalization.CultureInfo.InvariantCulture) + "'"; break;
                    case ParadoxFieldTypes.Timestamp: lit = "'" + ((DateTime)v).ToString("MM/dd/yyyy HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) + "'"; break;
                    default: lit = Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture); break;
                }
                names.Add("\"" + c.Name + "\"");
                lits.Add(lit);
            }
            string sql = "INSERT INTO '" + dbPath + "' (" + string.Join(", ", names) + ") VALUES (" + string.Join(", ", lits) + ")";
            var r = SqlRunner.Execute(sql, lockFileFolder: Path.GetDirectoryName(dbPath), timeoutMs: 15000);
            if (!r.Exited) throw new Exception("SQLRunner timed out (killed)");
            if (r.Stdout.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0 || r.Stdout.IndexOf("exception", StringComparison.OrdinalIgnoreCase) >= 0)
                throw new Exception("SQLRunner failed: " + r.Stdout.Trim());
        }

        private static void Restore(string backupFolder, string folder, string tableName)
        {
            BinaryComparisonTest.DeleteTableSet(folder, tableName);
            BinaryComparisonTest.CopyTableSet(backupFolder, tableName, folder, tableName);
        }

        private static void Log(StringBuilder report, string line)
        {
            report.AppendLine(line);
            Console.WriteLine("[randomtest] " + line);
        }
    }
}
