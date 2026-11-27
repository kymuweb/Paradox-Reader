using System;
using System.IO;
using System.Linq;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Mimics the ClientVersionLog service: single writer, open table, append ~batch rows, dispose, repeat.
    /// After every batch checks DB/XGn record counts and PX/XGn counters; stops at the first divergence
    /// and keeps before/after snapshots.
    /// Usage: ParadoxTest servicesimtest [srcDbPath] [batches=150] [batchSize=100]
    /// </summary>
    internal static class ServiceSimTest
    {
        public static int Run(string srcDb, int batches, int batchSize, string replayDb = null)
        {
            string dir = Path.Combine(@"C:\temp", "servicesim_" + DateTime.Now.ToString("yyyyMMdd_HHmmss"));
            Directory.CreateDirectory(dir);
            string srcBase = Path.GetFileNameWithoutExtension(srcDb);
            foreach (var f in Directory.GetFiles(Path.GetDirectoryName(srcDb), srcBase + ".*"))
            {
                string ext = Path.GetExtension(f);
                if (string.Equals(ext, ".LCK", StringComparison.OrdinalIgnoreCase)) continue;
                File.Copy(f, Path.Combine(dir, "T" + ext));
            }
            string db = Path.Combine(dir, "T.DB");
            object[] template;
            using (var t = new ParadoxTableFile(db)) template = t.Enumerate().First().DataValues;

            var rng = new Random(12345);
            int n = 0;
            System.Collections.Generic.List<object[]> replay = null;
            if (replayDb != null)
            {
                int start;
                using (var t = new ParadoxTableFile(db)) start = t.RecordCount;
                using (var r = new ParadoxTableFile(replayDb))
                    replay = r.Enumerate().Skip(start).Select(x => (object[])x.DataValues.Clone()).ToList();
                batches = (replay.Count + batchSize - 1) / batchSize;
                Console.WriteLine($"[servicesim] replaying {replay.Count} rows from {replayDb}");
            }
            for (int b = 0; b < batches; b++)
            {
                string prev = Path.Combine(dir, "prev");
                Snapshot(dir, prev);
                using (var t = new ParadoxTableFile(db))
                {
                    for (int i = 0; i < batchSize; i++, n++)
                    {
                        if (replay != null)
                        {
                            if (n >= replay.Count) break;
                            var rv = (object[])replay[n].Clone();
                            rv[0] = 0;
                            t.AppendRecord(rv);
                            continue;
                        }
                        var values = (object[])template.Clone();
                        values[0] = 0;
                        for (int k = 1; k < values.Length; k++)
                        {
                            if (values[k] is string s && s.Length > 0)
                            {
                                string v = rng.Next(0, 4) == 0 ? "V" + rng.Next(1, 40) : s;
                                values[k] = v.Length > s.Length ? v.Substring(0, s.Length) : v;
                            }
                            else if (values[k] is int) values[k] = rng.Next(1, 600);
                        }
                        t.AppendRecord(values);
                    }
                }
                string err = Check(dir);
                Console.WriteLine($"[servicesim] batch {b} rows={n} {Describe(dir)}{(err == null ? "" : " FAIL: " + err)}");
                if (err != null)
                {
                    Snapshot(dir, Path.Combine(dir, "bad"));
                    Console.WriteLine("[servicesim] snapshots: " + dir + @"\prev, \bad");
                    return 1;
                }
            }
            Console.WriteLine("[servicesim] PASS (internal invariants) output: " + dir);
            return 0;
        }

        private static void Snapshot(string dir, string to)
        {
            Directory.CreateDirectory(to);
            foreach (var f in Directory.GetFiles(dir, "T.*"))
                File.Copy(f, Path.Combine(to, Path.GetFileName(f)), true);
        }

        private static byte[] Head(string path)
        {
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var h = new byte[0x80];
                fs.Read(h, 0, h.Length);
                return h;
            }
        }

        private static string Describe(string dir)
        {
            var db = Head(Path.Combine(dir, "T.DB"));
            var px = Head(Path.Combine(dir, "T.PX"));
            string s = $"db.rc={BitConverter.ToInt32(db, 6)} db70={BitConverter.ToUInt16(db, 0x70)} px2C={px[0x2C]:X2} px49={BitConverter.ToInt32(px, 0x49)}";
            foreach (var x in Directory.GetFiles(dir, "T.XG*").OrderBy(p => p))
            {
                var h = Head(x);
                s += $" {Path.GetExtension(x)}.rc={BitConverter.ToInt32(h, 6)}/2F={h[0x2F]:X2}/len={new FileInfo(x).Length}";
            }
            return s;
        }

        private static string Check(string dir)
        {
            int dbRc;
            int chain;
            using (var t = new ParadoxTableFile(Path.Combine(dir, "T.DB")))
            {
                dbRc = t.RecordCount;
                chain = t.Enumerate().Count();
            }
            if (chain != dbRc) return $"DB chain {chain} != header {dbRc}";
            var px = Head(Path.Combine(dir, "T.PX"));
            foreach (var x in Directory.GetFiles(dir, "T.XG*"))
            {
                var h = Head(x);
                int rc = BitConverter.ToInt32(h, 6);
                if (rc != dbRc) return $"{Path.GetFileName(x)} rc {rc} != DB {dbRc}";
                if (h[0x2F] != px[0x2C]) return $"{Path.GetFileName(x)} 2F {h[0x2F]:X2} != PX 2C {px[0x2C]:X2}";
            }
            return null;
        }
    }
}
