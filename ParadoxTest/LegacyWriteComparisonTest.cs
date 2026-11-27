using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxTest
{
    /// <summary>
    /// Appends the same row(s) to a real table using (1) the legacy WriteRows tool (known good - legacy
    /// libraries open the result without "index out of date") and (2) ParadoxReader.ParadoxTableFile.AppendRecord,
    /// restoring the table between runs, then binary-compares the resulting table file sets and writes a
    /// report to the temp folder.
    /// </summary>
    internal static class LegacyWriteComparisonTest
    {
        public static void Run(string databaseFolderPath = null, string tableName = "ClientVersionLog", int writeCount = 10)
        {
            databaseFolderPath = databaseFolderPath ?? LegacyWriteRows.LegacyWriteRowsDatabaseFolderPath;

            var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var tempRoot = Path.Combine(@"C:\temp", "paradoxwritetest_" + stamp);
            var backupFolder = Path.Combine(tempRoot, "backup");
            var tablePath = Path.Combine(databaseFolderPath, tableName + ".db");

            Directory.CreateDirectory(tempRoot);
            Console.WriteLine($"[legacywritetest] working folder: {tempRoot}");

            // Private backup of the original table set (exact-name match only, so ClientVersionLog.bak.* is not included).
            var backedUp = BinaryComparisonTest.CopyTableSet(databaseFolderPath, tableName, backupFolder, tableName);
            Console.WriteLine($"[legacywritetest] backed up {backedUp.Count} files to {backupFolder}");
            if (backedUp.Count == 0) throw new FileNotFoundException("No table files found to back up", tablePath);

            var rows = BuildRows(writeCount);
            var report = new StringBuilder();
            string legacyName = tableName + "_legacy_" + stamp;
            string readerName = tableName + "_paradoxreader_" + stamp;
            string baselineName = tableName + "_baseline_" + stamp;

            try
            {
                BinaryComparisonTest.CopyTableSet(backupFolder, tableName, tempRoot, baselineName);

                // ---- 1. legacy write ----------------------------------------------------
                Console.WriteLine("[legacywritetest] legacy WriteRows...");
                LegacyWriteRows.WriteRows(rows.Select(r => (tableName + ".db", r)).ToList());
                BinaryComparisonTest.CopyTableSet(databaseFolderPath, tableName, tempRoot, legacyName);
                Restore(backupFolder, databaseFolderPath, tableName);

                // ---- 2. ParadoxReader write ---------------------------------------------
                Console.WriteLine("[legacywritetest] ParadoxReader AppendRecord...");
                try
                {
                    using (var table = new ParadoxReader.ParadoxTableFile(tablePath))
                    {
                        foreach (var row in rows)
                            table.AppendRecord(row.Values.ToArray());
                    }
                }
                catch (Exception ex)
                {
                    report.AppendLine("ParadoxReader append FAILED: " + ex);
                    Console.WriteLine("[legacywritetest] ParadoxReader append FAILED: " + ex.Message);
                }
                BinaryComparisonTest.CopyTableSet(databaseFolderPath, tableName, tempRoot, readerName);

                // Proof of index validity: the legacy tool must still open and append to the ParadoxReader output.
                try
                {
                    LegacyWriteRows.WriteRows(BuildRows(1).Select(r => (tableName + ".db", r)).ToList());
                    report.AppendLine("LEGACY OPEN/APPEND OF PARADOXREADER OUTPUT: OK");
                    Console.WriteLine("[legacywritetest] legacy can open+append to ParadoxReader output: OK");
                }
                catch (Exception ex)
                {
                    report.AppendLine("LEGACY OPEN/APPEND OF PARADOXREADER OUTPUT: FAILED - " + ex.Message);
                    Console.WriteLine("[legacywritetest] legacy can open+append to ParadoxReader output: FAILED - " + ex.Message);
                }
                Restore(backupFolder, databaseFolderPath, tableName);

                // ---- 3. compare ----------------------------------------------------------
                bool baseVsLegacy = BinaryComparisonTest.CompareTableSets(tempRoot, baselineName, tempRoot, legacyName, "baseline", "legacy", report);
                bool baseVsReader = BinaryComparisonTest.CompareTableSets(tempRoot, baselineName, tempRoot, readerName, "baseline", "paradoxreader", report);
                bool legacyVsReader = BinaryComparisonTest.CompareTableSets(tempRoot, legacyName, tempRoot, readerName, "legacy", "paradoxreader", report);

                var reportPath = Path.Combine(tempRoot, "differences_" + stamp + ".txt");
                bool logicalSame = CompareLogicalRows(tempRoot, legacyName, readerName, report);
                Console.WriteLine($"[legacywritetest] logical rows identical (legacy vs paradoxreader): {logicalSame}");
                File.WriteAllText(reportPath, report.ToString());
                Console.WriteLine($"[legacywritetest] legacy vs baseline identical: {baseVsLegacy}");
                Console.WriteLine($"[legacywritetest] paradoxreader vs baseline identical: {baseVsReader}");
                Console.WriteLine($"[legacywritetest] legacy vs paradoxreader identical: {legacyVsReader}");
                Console.WriteLine($"[legacywritetest] report: {reportPath}");
            }
            finally
            {
                // Always put the original back, even if something above threw.
                Restore(backupFolder, databaseFolderPath, tableName);
                Console.WriteLine("[legacywritetest] original table restored");
            }
        }

        private static List<string> ReadAllRows(string folder, string tableName)
        {
            var rows = new List<string>();
            using (var table = new ParadoxReader.ParadoxTableFile(Path.Combine(folder, tableName + ".DB")))
            {
                foreach (var rec in table.Enumerate())
                    rows.Add(string.Join("|", rec.DataValues.Select(v => v == null ? "<null>" : v.ToString())));
            }
            return rows;
        }

        /// <summary>
        /// Block packing may legitimately differ between writers, so compare the logical row sets
        /// (order-insensitive) read back through ParadoxReader from each output.
        /// </summary>
        private static bool CompareLogicalRows(string folder, string legacyName, string readerName, StringBuilder report)
        {
            try
            {
                var a = ReadAllRows(folder, legacyName);
                var b = ReadAllRows(folder, readerName);
                var onlyA = a.GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count());
                foreach (var x in b)
                {
                    int c;
                    if (onlyA.TryGetValue(x, out c)) onlyA[x] = c - 1; else onlyA[x] = -1;
                }
                var diffs = onlyA.Where(kv => kv.Value != 0).Take(20).ToList();
                report.AppendLine($"LOGICAL ROWS: legacy={a.Count} paradoxreader={b.Count} differingDistinctRows={onlyA.Count(kv => kv.Value != 0)}");
                foreach (var d in diffs) report.AppendLine($"  {d.Value:+#;-#}  {d.Key}");
                return a.Count == b.Count && diffs.Count == 0;
            }
            catch (Exception ex)
            {
                report.AppendLine("LOGICAL ROWS compare FAILED: " + ex);
                return false;
            }
        }

        private static void Restore(string backupFolder, string databaseFolderPath, string tableName)
        {
            BinaryComparisonTest.DeleteTableSet(databaseFolderPath, tableName);
            BinaryComparisonTest.CopyTableSet(backupFolder, tableName, databaseFolderPath, tableName);
        }

        /// <summary>
        /// Builds the rows to write. Id is left 0 (autoinc, assigned by the database). Values are randomised
        /// once so BOTH writers append identical data and the outputs are directly comparable.
        /// </summary>
        private static List<Dictionary<string, object>> BuildRows(int count)
        {
            var rng = new Random();
            var now = DateTime.Now;
            var rows = new List<Dictionary<string, object>>();
            for (int i = 0; i < count; i++)
            {
                var row = new Dictionary<string, object>();
                row.Add("Id", 0);
                row.Add("MajorVersion", rng.Next(0, 30));
                row.Add("MinorVersion", rng.Next(0, 100));
                row.Add("VersionName", "TestVersion" + rng.Next(1, 100));
                row.Add("VersionBuildDate", new DateTime(2024, 1, 15).AddDays(rng.Next(0, 300)));
                row.Add("ComputerName", "TESTPC" + rng.Next(1, 10));
                row.Add("UserName", "testuser" + rng.Next(1, 10));
                row.Add("ServerBaseDir", "C:\\Test");
                row.Add("ClientId", rng.Next(1, 5000));
                row.Add("OperatingSystem", "Windows 11");
                row.Add("DateTime", now.AddSeconds(i));
                rows.Add(row);
            }
            return rows;
        }
    }
}
