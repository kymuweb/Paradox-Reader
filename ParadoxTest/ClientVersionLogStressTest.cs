using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Stress test on the real ClientVersionLog table name so LegacyWriteRows can participate.
    /// Backs up the live ClientVersionLog.*, copies ClientVersionLog.blankcopy.* in as ClientVersionLog.*,
    /// writes batches (legacy / reader / alternating), and after every batch snapshots the files,
    /// checks invariants and runs Pdxrbld on a separate copy. Stops at the first failing batch.
    /// The live table is always restored.
    /// Usage: ParadoxTest cvlstress [mode=reader|legacy|alt] [batches=120] [batchSize=100]
    /// </summary>
    internal static class ClientVersionLogStressTest
    {
        private const string Table = "ClientVersionLog";

        public static int Run(string mode, int batches, int batchSize)
        {
            string data = LegacyWriteRows.LegacyWriteRowsDatabaseFolderPath;
            if (string.IsNullOrEmpty(data))
            {
                Console.WriteLine("[cvlstress] LegacyWriteRowsDatabaseFolderPath not configured; aborting.");
                return 1;
            }
            string root = Path.Combine(@"C:\temp", "cvlstress_" + mode + "_" + DateTime.Now.ToString("yyyyMMdd_HHmmss"));
            string backup = Path.Combine(root, "backup");
            Directory.CreateDirectory(root);

            if (BinaryComparisonTest.CopyTableSet(data, Table, backup, Table).Count == 0)
                throw new FileNotFoundException("live table not found", Path.Combine(data, Table + ".DB"));
            Console.WriteLine($"[cvlstress] live table backed up to {backup}");

            string db = Path.Combine(data, Table + ".DB");
            var rng = new Random(20261106);
            int result = 0, written = 0;
            try
            {
                Copy(data, Table + ".blankcopy", data, Table);
                for (int b = 0; b < batches; b++)
                {
                    string writer = mode == "alt" ? (b % 2 == 0 ? "reader" : "legacy") : mode;
                    var rows = Enumerable.Range(0, batchSize).Select(i => BuildRow(rng, written + i)).ToList();

                    string error = null;
                    try
                    {
                        if (writer == "legacy")
                            LegacyWriteRows.WriteRows(rows.Select(r => (Table + ".db", r)));
                        else
                            using (var t = new ParadoxTableFile(db))
                                foreach (var r in rows)
                                {
                                    var v = new object[t.FieldNames.Length];
                                    for (int i = 0; i < v.Length; i++) v[i] = r.TryGetValue(t.FieldNames[i], out var o) ? o : null;
                                    t.AppendRecord(v);
                                }
                    }
                    catch (Exception ex) { error = "write exception: " + ex.GetType().Name + ": " + ex.Message; }
                    written += batchSize;

                    string snap = Path.Combine(root, $"batch_{b:D3}_{writer}");
                    Copy(data, Table, snap, Table);
                    error = error ?? Check(snap, written);
                    string rb = null;
                    if (error == null && Pdxrbld.IsAvailable)
                    {
                        string rbDir = Path.Combine(root, "rbld");
                        if (Directory.Exists(rbDir)) Directory.Delete(rbDir, true);
                        Copy(snap, Table, rbDir, Table);
                        rb = Pdxrbld.Rebuild(rbDir, Table + ".DB", false) ? "pdxrbld ok" : "pdxrbld ERRORS";
                        if (rb != "pdxrbld ok") error = rb + " (see Pdxrbld.LOG)";
                    }
                    Console.WriteLine($"[cvlstress] batch {b} {writer} {Describe(snap)} {(error == null ? rb : "FAIL: " + error)}");
                    if (error != null) { result = 1; Console.WriteLine("[cvlstress] failing snapshot: " + snap); break; }
                    if (b > 1) Directory.Delete(Path.Combine(root, $"batch_{b - 2:D3}_{(mode == "alt" ? (b % 2 == 0 ? "reader" : "legacy") : mode)}"), true);
                }
                if (result == 0) Console.WriteLine("[cvlstress] PASS");
            }
            finally
            {
                foreach (var f in Directory.GetFiles(data, Table + ".*"))
                    if (Path.GetFileNameWithoutExtension(f).Equals(Table, StringComparison.OrdinalIgnoreCase)) File.Delete(f);
                Copy(backup, Table, data, Table);
                Console.WriteLine("[cvlstress] live table restored; output " + root);
            }
            return result;
        }

        private static void Copy(string srcFolder, string srcName, string destFolder, string destName)
        {
            Directory.CreateDirectory(destFolder);
            foreach (var f in Directory.GetFiles(srcFolder, srcName + ".*"))
            {
                if (!Path.GetFileNameWithoutExtension(f).Equals(srcName, StringComparison.OrdinalIgnoreCase)) continue;
                string ext = Path.GetExtension(f);
                if (ext.Equals(".LCK", StringComparison.OrdinalIgnoreCase)) continue;
                File.Copy(f, Path.Combine(destFolder, destName + ext), true);
            }
        }

        private static Dictionary<string, object> BuildRow(Random rng, int n)
        {
            int client = rng.Next(1, 400);
            return new Dictionary<string, object>
            {
                { "Id", 0 },
                { "MajorVersion", rng.Next(30, 33) },
                { "MinorVersion", rng.Next(0, 40) },
                { "VersionName", "32." + rng.Next(0, 40) + "." + rng.Next(0, 9) },
                { "VersionBuildDate", new DateTime(2026, 1, 1).AddDays(rng.Next(0, 300)) },
                { "ComputerName", "PC-" + client + "-" + rng.Next(1, 6) },
                { "UserName", "user" + rng.Next(1, 900) },
                { "ServerBaseDir", @"C:\Server\Base\Dir" },
                { "ClientId", client },
                { "OperatingSystem", rng.Next(0, 3) == 0 ? "Windows 10" : "Windows 11" },
                { "DateTime", new DateTime(2026, 11, 6, 8, 0, 0).AddSeconds(n) },
            };
        }

        private static byte[] Head(string path)
        {
            var h = new byte[0x80];
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) fs.Read(h, 0, h.Length);
            return h;
        }

        private static string Describe(string dir)
        {
            var db = Head(Path.Combine(dir, Table + ".DB"));
            var px = Head(Path.Combine(dir, Table + ".PX"));
            string s = $"rc={BitConverter.ToInt32(db, 6)} db49={BitConverter.ToInt32(db, 0x49)} db2D={db[0x2D]:X2} px2C={px[0x2C]:X2} px49={BitConverter.ToInt32(px, 0x49)}";
            foreach (var ext in new[] { "XG0", "XG1", "YG0", "YG1" })
            {
                var h = Head(Path.Combine(dir, Table + "." + ext));
                s += $" {ext}:rc={BitConverter.ToInt32(h, 6)},2C={h[0x2C]:X2},2F={h[0x2F]:X2},49={BitConverter.ToInt32(h, 0x49)}";
            }
            return s;
        }

        private static string Check(string dir, int expected)
        {
            using (var t = new ParadoxTableFile(Path.Combine(dir, Table + ".DB")))
            {
                var ids = t.Enumerate().Select(r => Convert.ToInt64(r.DataValues[0])).ToList();
                if (t.RecordCount != expected) return $"DB header rc {t.RecordCount} != expected {expected}";
                if (ids.Count != expected) return $"DB chain rows {ids.Count} != expected {expected}";
                if (ids.Distinct().Count() != ids.Count) return "duplicate Ids";
            }
            var px = Head(Path.Combine(dir, Table + ".PX"));
            foreach (var ext in new[] { "XG0", "XG1" })
            {
                var h = Head(Path.Combine(dir, Table + "." + ext));
                if (BitConverter.ToInt32(h, 6) != expected) return $"{ext} rc {BitConverter.ToInt32(h, 6)} != {expected}";
            }
            return null;
        }
    }
}
