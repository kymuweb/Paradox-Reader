using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// For each of the three writers (ParadoxReader, SQLRunner, legacy WriteRows) writes rows one at a time into a
    /// real table in the legacy data folder (backed up first and always restored), and after each write:
    ///   1. looks the new row up through the primary key and every secondary index using ParadoxReader,
    ///   2. snapshots every table file and binary-diffs the index files against the previous snapshot.
    /// Control lookups of pre-existing rows separate "writer problem" from "reader/lookup problem".
    /// Writers run strictly sequentially (external tools are killed on timeout by their helpers).
    /// Usage: ParadoxTest writeindextest [rounds=5] [tableName=ClientVersionLog]
    /// </summary>
    internal static class IndexWriteLookupTest
    {
        private static readonly string[] SqlSkipColumns = { "Id", "VersionBuildDate", "DateTime" };

        public static void Run(int rounds = 5, string tableName = "ClientVersionLog", string[] writers = null, bool quiet = false)
        {
            string dataFolder = LegacyWriteRows.LegacyWriteRowsDatabaseFolderPath;
            string stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string tempRoot = Path.Combine(@"C:\temp", "writeindextest_" + stamp);
            string backup = Path.Combine(tempRoot, "backup");
            string tablePath = Path.Combine(dataFolder, tableName + ".DB");
            var report = new StringBuilder();

            Directory.CreateDirectory(tempRoot);
            int backedUp = BinaryComparisonTest.CopyTableSet(dataFolder, tableName, backup, tableName).Count;
            if (backedUp == 0) throw new FileNotFoundException("No table files found", tablePath);
            Log(report, "working folder: " + tempRoot + " (backed up " + backedUp + " files)");

            try
            {
                foreach (string writer in writers ?? new[] { "paradoxreader", "sqlrunner", "legacy" })
                {
                    if (writer == "sqlrunner" && !SqlRunner.IsAvailable) { Log(report, "SKIP sqlrunner (not configured)"); continue; }
                    if (writer == "legacy" && string.IsNullOrEmpty(LegacyWriteRows.LegacyWriteRowsExeFilePath)) { Log(report, "SKIP legacy (not configured)"); continue; }

                    Restore(backup, dataFolder, tableName);
                    Log(report, "");
                    Log(report, "================ WRITER: " + writer + " ================");

                    var written = new List<object[]>();
                    var prev = Snapshot(dataFolder, tableName);
                    var controls = PickControls(tablePath);
                    LoadTemplate(tablePath);
                    Log(report, "control rows (pre-existing): " + controls.Count);
                    ReportLookups(report, tablePath, controls, "control(pre-write)");

                    for (int i = 1; i <= rounds; i++)
                    {
                        if (quiet)
                        {
                            try { WriteOne(writer, tablePath, tableName, i, report); }
                            catch (Exception ex) { Log(report, "WRITE FAILED #" + i + ": " + ex.Message); break; }
                            if (i % 50 == 0 || i == rounds) Log(report, "  " + writer + " wrote " + i + "/" + rounds);
                            if (i == rounds)
                            {
                                prev = Snapshot(dataFolder, tableName);
                                Log(report, "row count now " + CountRows(tablePath));
                                ReportLookups(report, tablePath, controls, "control");
                            }
                            continue;
                        }
                        Log(report, "--- " + writer + " write #" + i + " ---");
                        int before = CountRows(tablePath);
                        try { WriteOne(writer, tablePath, tableName, i, report); }
                        catch (Exception ex) { Log(report, "WRITE FAILED: " + ex.Message); break; }
                        int after = CountRows(tablePath);
                        Log(report, "row count " + before + " -> " + after);

                        var newRow = FindNewestRow(tablePath);
                        if (newRow != null) written.Add(newRow);
                        var now = Snapshot(dataFolder, tableName);
                        ReportDiff(report, prev, now);
                        prev = now;

                        ReportLookups(report, tablePath, new List<object[]>(written), "written-so-far");
                        ReportLookups(report, tablePath, controls, "control");
                    }
                    SaveSnapshot(tempRoot, writer, prev);
                }
            }
            finally
            {
                Restore(backup, dataFolder, tableName);
                Log(report, "");
                Log(report, "original table restored");
                string path = Path.Combine(tempRoot, "report.txt");
                File.WriteAllText(path, report.ToString());
                Console.WriteLine("[writeindextest] report: " + path);
            }
        }

        private static void WriteOne(string writer, string tablePath, string tableName, int n, StringBuilder report)
        {
            var row = BuildRow(writer, n);
            if (writer == "legacy")
            {
                LegacyWriteRows.WriteRow(tableName + ".db", row);
            }
            else if (writer == "paradoxreader")
            {
                using (var t = new ParadoxTableFile(tablePath))
                {
                    var values = new object[t.FieldNames.Length];
                    for (int i = 0; i < values.Length; i++)
                    {
                        object v;
                        values[i] = row.TryGetValue(t.FieldNames[i], out v) ? v : null;
                    }
                    t.AppendRecord(values);
                }
            }
            else
            {
                var cols = new List<string>();
                var lits = new List<string>();
                foreach (var kv in row)
                {
                    if (SqlSkipColumns.Contains(kv.Key, StringComparer.OrdinalIgnoreCase)) continue;
                    cols.Add(kv.Key);
                    lits.Add(kv.Value is string ? "'" + ((string)kv.Value).Replace("'", "''") + "'" : Convert.ToString(kv.Value, System.Globalization.CultureInfo.InvariantCulture));
                }
                string sql = "INSERT INTO '" + tablePath + "' (" + string.Join(", ", cols) + ") VALUES (" + string.Join(", ", lits) + ")";
                var r = SqlRunner.Execute(sql, lockFileFolder: Path.GetDirectoryName(tablePath), timeoutMs: 10000);
                if (!r.Exited) throw new Exception("SQLRunner timed out (killed)");
                if (r.Stdout.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0)
                    Log(report, "  sqlrunner stdout: " + r.Stdout.Trim());
            }
        }

        private static Dictionary<string, object> BuildRow(string writer, int n)
        {
            var rng = new Random(unchecked(writer.GetHashCode() * 31 + n));
            string tag = writer.Substring(0, 3).ToUpperInvariant() + n.ToString("D3") + rng.Next(100, 999);
            if (templateRow != null)
            {
                var d = new Dictionary<string, object>();
                for (int i = 0; i < templateNames.Length; i++)
                {
                    object v = templateRow[i];
                    if (i == 0 && (v is int || v is long || v is short)) v = 0;
                    else if (v is string) { string s = (string)v; int max = Math.Max(s.Length, 8); string nv = (tag + s); v = nv.Length > max ? nv.Substring(0, max) : nv; }
                    else if (v is int) v = rng.Next(1, 3000);
                    else if (v is short) v = (short)rng.Next(1, 3000);
                    else if (v is long) v = (long)rng.Next(1, 3000);
                    d[templateNames[i]] = v;
                }
                return d;
            }
            return new Dictionary<string, object>
            {
                { "Id", 0 },
                { "MajorVersion", rng.Next(0, 30) },
                { "MinorVersion", rng.Next(0, 100) },
                { "VersionName", "IDX" + tag },
                { "VersionBuildDate", new DateTime(2024, 1, 15).AddDays(rng.Next(0, 300)) },
                { "ComputerName", "PC" + tag },
                { "UserName", "user" + tag },
                { "ServerBaseDir", "C:\\Test" },
                { "ClientId", rng.Next(1, 5000) },
                { "OperatingSystem", "Windows 11" },
                { "DateTime", DateTime.Now.AddSeconds(n) },
            };
        }

        private static object[] templateRow;
        private static string[] templateNames;

        // Uses an existing row of the (populated) table as the shape for new rows. Skipped when the table is empty.
        private static void LoadTemplate(string tablePath)
        {
            templateRow = null;
            using (var t = new ParadoxTableFile(tablePath))
            {
                var first = t.Enumerate().FirstOrDefault();
                if (first == null) return;
                templateRow = first.DataValues.ToArray();
                templateNames = t.FieldNames.ToArray();
            }
        }

        // ---- reading / lookups ----

        private static int CountRows(string tablePath)
        {
            using (var t = new ParadoxTableFile(tablePath)) return t.Enumerate().Count();
        }

        private static object[] FindNewestRow(string tablePath)
        {
            using (var t = new ParadoxTableFile(tablePath))
            {
                object[] best = null;
                long bestId = long.MinValue;
                foreach (var rec in t.Enumerate())
                {
                    long id = Convert.ToInt64(rec.DataValues[0]);
                    if (id >= bestId) { bestId = id; best = rec.DataValues.ToArray(); }
                }
                return best;
            }
        }

        private static List<object[]> PickControls(string tablePath)
        {
            using (var t = new ParadoxTableFile(tablePath))
            {
                var all = t.Enumerate().ToList();
                var picks = new List<object[]>();
                if (all.Count == 0) return picks;
                foreach (int idx in new[] { 0, all.Count / 4, all.Count / 2, all.Count * 3 / 4, all.Count - 1 }.Distinct())
                    picks.Add(all[idx].DataValues.ToArray());
                return picks;
            }
        }

        private static void ReportLookups(StringBuilder report, string tablePath, List<object[]> rows, string label)
        {
            if (rows.Count == 0) return;
            using (var t = new ParadoxTableFile(tablePath))
            {
                var sb = new StringBuilder();
                sb.Append("  lookups [" + label + "] ");

                int pkFound = 0;
                if (t.PrimaryKeyIndex != null)
                {
                    foreach (var r in rows)
                        if (t.PrimaryKeyIndex.Enumerate(new ParadoxCondition.Compare(ParadoxCompareOperator.Equal, r[0], 0, 0)).Any(x => Convert.ToInt64(x.DataValues[0]) == Convert.ToInt64(r[0]))) pkFound++;
                    sb.Append("PK " + pkFound + "/" + rows.Count);
                }
                else sb.Append("PK n/a");

                foreach (var ix in t.SecondaryIndexes)
                {
                    int found = 0; string err = null;
                    int f0 = ix.FieldIndices[0];
                    try {
                    foreach (var r in rows)
                    {
                        var cond = new ParadoxCondition.Compare(ParadoxCompareOperator.Equal, r[f0], f0, 0);
                        if (ix.Enumerate(cond).Any(x => Convert.ToInt64(x.DataValues[0]) == Convert.ToInt64(r[0]))) found++;
                    }
                    } catch (Exception ex) { err = " EXC:" + ex.Message.Substring(0, Math.Min(60, ex.Message.Length)); }
                    sb.Append("; " + Path.GetExtension(ix.FilePath) + "(" + string.Join("+", ix.FieldIndices.Select(k => t.FieldNames[k])) + ") " + found + "/" + rows.Count + err);
                }
                Log(report, sb.ToString());
            }
        }

        // ---- binary snapshots / diffs --------------------------------------------------------------

        private static Dictionary<string, byte[]> Snapshot(string folder, string tableName)
        {
            var d = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
            foreach (var f in BinaryComparisonTest.FindTableFiles(folder, tableName))
                d[Path.GetExtension(f)] = File.ReadAllBytes(f);
            return d;
        }

        private static void SaveSnapshot(string root, string writer, Dictionary<string, byte[]> snap)
        {
            string dir = Path.Combine(root, "final_" + writer);
            Directory.CreateDirectory(dir);
            foreach (var kv in snap) File.WriteAllBytes(Path.Combine(dir, "T" + kv.Key), kv.Value);
        }

        private static void ReportDiff(StringBuilder report, Dictionary<string, byte[]> a, Dictionary<string, byte[]> b)
        {
            foreach (var ext in b.Keys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase))
            {
                byte[] x; a.TryGetValue(ext, out x);
                byte[] y = b[ext];
                if (x == null) { Log(report, "  " + ext + ": NEW file (" + y.Length + " bytes)"); continue; }
                int min = Math.Min(x.Length, y.Length);
                int diff = 0;
                var chunks = new SortedSet<int>();
                for (int i = 0; i < min; i++)
                    if (x[i] != y[i]) { diff++; chunks.Add(i / 1024); }
                if (x.Length == y.Length && diff == 0) continue;
                string lenInfo = x.Length == y.Length ? "len " + y.Length : "len " + x.Length + " -> " + y.Length;
                Log(report, "  " + ext + ": " + diff + " bytes differ, " + lenInfo + ", changed 1K-chunks [" + string.Join(",", chunks.Take(24)) + (chunks.Count > 24 ? ",..." : "") + "]");
            }
        }

        private static void Restore(string backup, string dataFolder, string tableName)
        {
            BinaryComparisonTest.DeleteTableSet(dataFolder, tableName);
            BinaryComparisonTest.CopyTableSet(backup, tableName, dataFolder, tableName);
        }

        private static void Log(StringBuilder report, string line)
        {
            Console.WriteLine(line);
            report.AppendLine(line);
        }
    }
}
