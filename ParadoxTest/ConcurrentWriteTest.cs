using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Reproduces the multi-writer ClientVersionLog corruption: several processes each keep one
    /// long-lived ParadoxTableFile open and append rows in a loop against the same table copy.
    /// Usage: ParadoxTest concurrenttest [srcDbPath] [processes=3] [rowsPerProcess=300]
    ///        ParadoxTest concurrentworker dbPath rows workerId
    /// </summary>
    internal static class ConcurrentWriteTest
    {
        public static int Run(string srcDb, int processes, int rows)
        {
            string dir = Path.Combine(@"C:\temp", "concurrenttest_" + DateTime.Now.ToString("yyyyMMdd_HHmmss"));
            Directory.CreateDirectory(dir);
            string srcBase = Path.Combine(Path.GetDirectoryName(srcDb), Path.GetFileNameWithoutExtension(srcDb));
            foreach (var f in Directory.GetFiles(Path.GetDirectoryName(srcDb), Path.GetFileName(srcBase) + ".*"))
            {
                string ext = Path.GetExtension(f);
                if (string.Equals(ext, ".LCK", StringComparison.OrdinalIgnoreCase)) continue;
                File.Copy(f, Path.Combine(dir, "T" + ext));
            }
            string db = Path.Combine(dir, "T.DB");
            int before;
            using (var t = new ParadoxTableFile(db)) before = t.RecordCount;
            Console.WriteLine($"[concurrenttest] {db} start={before} processes={processes} rows={rows}");

            string exe = Process.GetCurrentProcess().MainModule.FileName;
            var procs = new List<Process>();
            for (int p = 0; p < processes; p++)
            {
                var psi = new ProcessStartInfo(exe, $"concurrentworker \"{db}\" {rows} {p}") { UseShellExecute = false };
                procs.Add(Process.Start(psi));
            }
            foreach (var p in procs) p.WaitForExit();
            int failedWorkers = procs.Count(p => p.ExitCode != 0);

            int headerCount, chainRows, dups;
            using (var t = new ParadoxTableFile(db))
            {
                headerCount = t.RecordCount;
                var ids = t.Enumerate().Select(r => Convert.ToInt64(r.DataValues[0])).ToList();
                chainRows = ids.Count;
                dups = ids.GroupBy(i => i).Count(g => g.Count() > 1);
                int unsorted = 0;
                for (int i = 1; i < ids.Count; i++) if (ids[i] <= ids[i - 1]) unsorted++;
                Console.WriteLine($"[concurrenttest] expected={before + processes * rows} header={headerCount} enumerated={chainRows} dupIds={dups} orderViolations={unsorted} failedWorkers={failedWorkers}");
                bool ok = headerCount == before + processes * rows && chainRows == headerCount && dups == 0 && unsorted == 0 && failedWorkers == 0;
                Console.WriteLine(ok ? "[concurrenttest] PASS" : "[concurrenttest] FAIL");
                Console.WriteLine("[concurrenttest] output: " + dir + " (run pdxrbld on T.DB for index checks)");
                return ok ? 0 : 1;
            }
        }

        public static int Worker(string db, int rows, int workerId)
        {
            try
            {
                var rng = new Random(workerId * 7919 + 1);
                using (var t = new ParadoxTableFile(db))
                {
                    var template = t.Enumerate().First().DataValues;
                    for (int n = 0; n < rows; n++)
                    {
                        var values = (object[])template.Clone();
                        values[0] = 0;
                        for (int i = 1; i < values.Length; i++)
                        {
                            if (values[i] is string s) values[i] = ("W" + workerId + "-" + n + " " + s).Substring(0, Math.Min(s.Length + 8, Math.Max(s.Length, 8)));
                            else if (values[i] is int) values[i] = rng.Next(1, 3000);
                        }
                        t.AppendRecord(values);
                    }
                }
                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[worker {workerId}] {ex}");
                return 1;
            }
        }
    }
}
