using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// For every *.db table in the legacy data folder, appends ONE identical row with
    /// (1) the legacy WriteRows tool and (2) ParadoxReader, running Pdxrbld after each, and keeps
    /// snapshots of every stage so index (PX/Xnn/Ynn) variances can be compared:
    ///   original -> legacy -> legacy_rebuilt -> (restore) -> reader -> reader_rebuilt -> (restore)
    /// Output: C:\temp\alltablewrite_[stamp]\[table]\... plus summary.csv / report.txt.
    /// </summary>
    internal static class AllTableWriteComparisonTest
    {
        private const int WriteTimeoutMs = 5000;
        private static readonly string[] ToolProcessNames = { "WriteRows", "Pdxrbld" };

        public static int Run(string dataFolder = null, string filter = null, int maxTables = int.MaxValue)
        {
            dataFolder = dataFolder ?? LegacyWriteRows.LegacyWriteRowsDatabaseFolderPath;
            if (!Pdxrbld.IsAvailable) { Console.WriteLine("[alltablewrite] Pdxrbld.exe not configured/found."); return 1; }

            var stamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var root = Path.Combine(@"C:\temp", "alltablewrite_" + stamp);
            Directory.CreateDirectory(root);
            var summaryPath = Path.Combine(root, "summary.csv");
            File.WriteAllText(summaryPath, "Table,DbBytes,Fields,PkFields,IndexFiles,LegacyWrite,LegacyRebuildClean,ReaderWrite,ReaderRebuildClean,DbSame,PxSame,SecondarySame,LegacyChangedByRebuild,ReaderChangedByRebuild,RebuiltSame,Notes" + Environment.NewLine);
            Console.WriteLine($"[alltablewrite] output: {root}");

            LegacyWriteRows.MaxAttempts = 1;           // never retry: a partial first attempt would double-write
            LegacyWriteRows.ProcessTimeoutMs = WriteTimeoutMs;
            LegacyWriteRows.InterWriteDelayMs = 500;

            var tables = Directory.GetFiles(dataFolder, "*.db")
                .Select(Path.GetFileNameWithoutExtension)
                .Where(t => !string.Equals(t, "Thumbs", StringComparison.OrdinalIgnoreCase))
                .Where(t => string.IsNullOrEmpty(filter) || t.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0)
                .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
                .Take(maxTables)
                .ToList();

            int n = 0;
            foreach (var table in tables)
            {
                n++;
                Console.WriteLine($"=== [{n}/{tables.Count}] {table} ===");
                string line;
                try { line = RunTable(dataFolder, table, Path.Combine(root, table)); }
                catch (Exception ex) { line = $"{table},,,,,,,,,,,,,,,\"FATAL {Csv(ex.Message)}\""; Console.WriteLine("  FATAL: " + ex.Message); }
                File.AppendAllText(summaryPath, line + Environment.NewLine);
            }
            Console.WriteLine($"[alltablewrite] done. summary: {summaryPath}");
            return 0;
        }

        private static string RunTable(string dataFolder, string table, string dir)
        {
            var original = Path.Combine(dir, "0_original");
            var report = new StringBuilder();
            var notes = new List<string>();
            Cleanup(dataFolder, table);

            if (BinaryComparisonTest.CopyTableSet(dataFolder, table, original, table).Count == 0)
                return $"{table},,,,,,,,,,,,,,,no files";

            long dbBytes = new FileInfo(Path.Combine(original, table + ".db")).Length;
            long familyBytes = BinaryComparisonTest.FindTableFiles(original, table).Sum(f => new FileInfo(f).Length);
            int rebuildTimeoutMs = (int)Math.Min(600000, 30000 + familyBytes / (10L * 1024 * 1024) * 30000);
            string indexFiles = string.Join(" ", BinaryComparisonTest.FindTableFiles(original, table)
                .Select(f => Path.GetExtension(f).TrimStart('.').ToUpperInvariant()).Where(e => e != "DB"));

            object[] row; string[] names; int pkFields; int fieldCount;
            try
            {
                using (var t = ParadoxTableFile.OpenReadOnly(Path.Combine(original, table + ".db")))
                {
                    if (t.IsEncrypted) notes.Add("encrypted");
                    names = t.FieldNames;
                    fieldCount = names.Length;
                    pkFields = t.primaryKeyFields;
                    row = BuildRow(t);
                }
            }
            catch (Exception ex)
            {
                return $"{table},{dbBytes},,,{indexFiles},,,,,,,,,,,\"read failed: {Csv(ex.Message)}\"";
            }
            File.WriteAllText(Path.Combine(dir, "row.txt"), string.Join(Environment.NewLine, names.Select((nm, i) => $"{nm} = {Fmt(row[i])}")));

            var dict = new Dictionary<string, object>();
            for (int i = 0; i < names.Length; i++) dict[names[i]] = row[i];

            string legacyOk, readerOk, legacyClean = "", readerClean = "";
            try
            {
                // ---- legacy ------------------------------------------------------------
                legacyOk = Stage(() => LegacyWriteRows.WriteRow(table + ".db", dict), WriteTimeoutMs * 2);
                Cleanup(dataFolder, table);
                BinaryComparisonTest.CopyTableSet(dataFolder, table, Path.Combine(dir, "1_legacy"), table);
                if (legacyOk == "OK") legacyClean = Rebuild(dataFolder, table, rebuildTimeoutMs, Path.Combine(dir, "1_legacy_pdxrbld.log"));
                BinaryComparisonTest.CopyTableSet(dataFolder, table, Path.Combine(dir, "2_legacy_rebuilt"), table);
                Restore(original, dataFolder, table);

                // ---- ParadoxReader -----------------------------------------------------
                readerOk = Stage(() =>
                {
                    using (var t = new ParadoxTableFile(Path.Combine(dataFolder, table + ".db")))
                        t.AppendRecord((object[])row.Clone());
                }, WriteTimeoutMs);
                Cleanup(dataFolder, table);
                BinaryComparisonTest.CopyTableSet(dataFolder, table, Path.Combine(dir, "3_reader"), table);
                if (readerOk == "OK") readerClean = Rebuild(dataFolder, table, rebuildTimeoutMs, Path.Combine(dir, "3_reader_pdxrbld.log"));
                BinaryComparisonTest.CopyTableSet(dataFolder, table, Path.Combine(dir, "4_reader_rebuilt"), table);
            }
            finally
            {
                Restore(original, dataFolder, table);
            }

            // ---- compare -----------------------------------------------------------------
            string L = Path.Combine(dir, "1_legacy"), LR = Path.Combine(dir, "2_legacy_rebuilt");
            string R = Path.Combine(dir, "3_reader"), RR = Path.Combine(dir, "4_reader_rebuilt");
            bool both = legacyOk == "OK" && readerOk == "OK";
            string dbSame = "", pxSame = "", secSame = "", lChg = "", rChg = "", rbSame = "";
            if (both)
            {
                BinaryComparisonTest.CompareTableSets(L, table, R, table, "legacy", "reader", report);
                dbSame = Same(L, R, table, f => f == ".db");
                pxSame = Same(L, R, table, f => f == ".px");
                secSame = Same(L, R, table, f => f.StartsWith(".x") || f.StartsWith(".y"));
                lChg = (!BinaryComparisonTest.CompareTableSets(L, table, LR, table, "legacy", "legacy_rebuilt", report)).ToString();
                rChg = (!BinaryComparisonTest.CompareTableSets(R, table, RR, table, "reader", "reader_rebuilt", report)).ToString();
                rbSame = BinaryComparisonTest.CompareTableSets(LR, table, RR, table, "legacy_rebuilt", "reader_rebuilt", report).ToString();
            }
            File.WriteAllText(Path.Combine(dir, "report.txt"), report.ToString());

            Console.WriteLine($"  legacy={legacyOk} rebuildClean={legacyClean} | reader={readerOk} rebuildClean={readerClean} | db={dbSame} px={pxSame} sec={secSame}");
            return string.Join(",", table, dbBytes, fieldCount, pkFields, indexFiles,
                Csv(legacyOk), legacyClean, Csv(readerOk), readerClean, dbSame, pxSame, secSame, lChg, rChg, rbSame,
                "\"" + Csv(string.Join("; ", notes)) + "\"");
        }

        /// <summary>Identical bytes for the files of the given extensions (case-insensitive). Empty when none exist.</summary>
        private static string Same(string a, string b, string table, Func<string, bool> extFilter)
        {
            var fa = BinaryComparisonTest.FindTableFiles(a, table).Where(f => extFilter(Path.GetExtension(f).ToLowerInvariant())).ToList();
            if (fa.Count == 0) return "";
            foreach (var f in fa)
            {
                var other = Path.Combine(b, Path.GetFileName(f));
                if (!File.Exists(other) || !File.ReadAllBytes(f).SequenceEqual(File.ReadAllBytes(other))) return "False";
            }
            return "True";
        }

		/// <summary>
		/// Builds a random row (tables are mostly empty, so existing data can't be used as a guide).
		/// Integer fields are sequential (max existing + 1), dates are random within +/-100 years of today,
		/// AutoInc is left 0 and BLOb-type fields are null.
		/// </summary>
		private static object[] BuildRow(ParadoxTableFile t)
		{
			var types = t.FieldTypes;
			var rng = new Random();
			var maxInt = new long[types.Length];
			foreach (var rec in t.Enumerate())
				for (int i = 0; i < types.Length; i++)
					if ((types[i].fType == ParadoxFieldTypes.Short || types[i].fType == ParadoxFieldTypes.Long) && rec.DataValues[i] != null)
						maxInt[i] = Math.Max(maxInt[i], Convert.ToInt64(rec.DataValues[i]));

			var row = new object[types.Length];
			for (int i = 0; i < types.Length; i++)
			{
				int size = types[i].fSize;
				switch (types[i].fType)
				{
					case ParadoxFieldTypes.AutoInc: row[i] = 0; break;
					case ParadoxFieldTypes.Short: row[i] = (short)Math.Min(short.MaxValue, maxInt[i] + 1); break;
					case ParadoxFieldTypes.Long: row[i] = (int)Math.Min(int.MaxValue, maxInt[i] + 1); break;
					case ParadoxFieldTypes.Alpha:
						const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
						int len = rng.Next(1, size + 1);
						row[i] = new string(Enumerable.Range(0, len).Select(_ => chars[rng.Next(chars.Length)]).ToArray());
						break;
					case ParadoxFieldTypes.Number: row[i] = Math.Round(rng.NextDouble() * 100000, 2); break;
					case ParadoxFieldTypes.Currency: case ParadoxFieldTypes.BCD: row[i] = Math.Round(rng.NextDouble() * 10000, 2); break;
					case ParadoxFieldTypes.Logical: row[i] = rng.Next(2) == 1; break;
					case ParadoxFieldTypes.Date: row[i] = DateTime.Today.AddDays(rng.Next(-36500, 36500)); break;
					case ParadoxFieldTypes.Timestamp:
						row[i] = DateTime.Today.AddDays(rng.Next(-36500, 36500)).AddSeconds(rng.Next(86400)); break;
					case ParadoxFieldTypes.Time: row[i] = TimeSpan.FromSeconds(rng.Next(86400)); break;
					default: row[i] = null; break;
				}
			}
			return row;
		}

        /// <summary>Runs an action with a timeout; returns "OK", "TIMEOUT" or the error message.</summary>
        private static string Stage(Action action, int timeoutMs)
        {
            var task = Task.Run(action);
            try
            {
                if (!task.Wait(timeoutMs))
                {
                    // never restore files underneath a still-running write
                    try { task.Wait(60000); } catch { }
                    return "TIMEOUT";
                }
                return "OK";
            }
            catch (AggregateException ex) { return "ERR: " + ex.InnerException?.Message; }
        }

        private static string Rebuild(string dataFolder, string table, int timeoutMs, string logCopyPath)
        {
            string logPath = Path.Combine(Path.GetDirectoryName(Pdxrbld.ExePath) ?? "", "Pdxrbld.LOG");
            long before = File.Exists(logPath) ? new FileInfo(logPath).Length : 0;
            bool clean = Pdxrbld.Rebuild(dataFolder, table + ".db", true, timeoutMs);
            if (!clean) { Cleanup(dataFolder, table); return Pdxrbld.LastExitCode == null ? "TIMEOUT" : Pdxrbld.LastExitCode == 0 ? "False" : $"CRASH 0x{Pdxrbld.LastExitCode:X8}"; }
            if (File.Exists(logPath))
            {
                using (var fs = new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    fs.Position = Math.Min(before, fs.Length);
                    using (var r = new StreamReader(fs)) File.WriteAllText(logCopyPath, r.ReadToEnd());
                }
            }
            Cleanup(dataFolder, table);
            return clean.ToString();
        }

        private static void Restore(string original, string dataFolder, string table)
        {
            Cleanup(dataFolder, table);
            for (int attempt = 1; ; attempt++)
            {
                try
                {
                    BinaryComparisonTest.DeleteTableSet(dataFolder, table);
                    BinaryComparisonTest.CopyTableSet(original, table, dataFolder, table);
                    return;
                }
                catch (IOException) when (attempt < 10) { System.Threading.Thread.Sleep(500); }
            }
        }

        /// <summary>Kills leftover WriteRows/Pdxrbld processes and removes *.lck files in the data folder.</summary>
        private static void Cleanup(string dataFolder, string table)
        {
            foreach (var name in ToolProcessNames)
                foreach (var p in Process.GetProcessesByName(name))
                {
                    try { Console.WriteLine($"  [cleanup] killing {name} ({p.Id})"); p.Kill(); p.WaitForExit(5000); } catch { }
                    finally { p.Dispose(); }
                }
            foreach (var lck in Directory.GetFiles(dataFolder, "*.lck"))
            {
                try { File.Delete(lck); } catch (Exception ex) { Console.WriteLine($"  [cleanup] could not delete {Path.GetFileName(lck)}: {ex.Message}"); }
            }
        }

        private static string Fmt(object v) => v == null ? "<null>" : $"{v} ({v.GetType().Name})";
        private static string Csv(string s) => (s ?? "").Replace("\"", "'").Replace(",", ";").Replace("\r", " ").Replace("\n", " ");
    }
}
