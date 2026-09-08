using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ParadoxTest
{
    /// <summary>
    /// Investigates how the BDE's idapi32.cfg "PARADOX LEVEL" / "BLOCK SIZE"
    /// defaults affect the on-disk header bytes of tables created purely via
    /// SQLRunner (i.e. a real, unmodified BDE), across every schema shape in
    /// <see cref="HeaderCompareTest.BuildCases"/>.
    ///
    /// For each case this creates the table via SQLRunner CREATE TABLE/CREATE
    /// INDEX once with idapi32.cfg pointed at the "original settings" backup
    /// (Paradox level 5, 2048-byte blocks) and once with it pointed at the
    /// "larger block size" backup (Paradox level 7, 32768-byte blocks), then
    /// byte-diffs every resulting header, annotating any diff that falls on a
    /// known <see cref="ParadoxReader.ParadoxHeaderOffsets"/> field.
    ///
    /// This is purely diagnostic (prints reports); the live idapi32.cfg is
    /// swapped out and restored to its original (pre-run) contents when done,
    /// even if the run throws partway through.
    /// </summary>
    internal static class BdeConfigCompareTest
    {
        private const string RootDir = @"c:\temp\bdeconfigcompare";
        private const string BdeDir = @"C:\Program Files\Borland\Common Files\BDE";
        private const string LiveCfgPath = BdeDir + @"\idapi32.cfg";
        private const string OriginalSettingsCfgPath = BdeDir + @"\idapi32.originalsettings.cfg";
        private const string LargerBlockSizeCfgPath = BdeDir + @"\Idapi32.largerpdoxblocksize.cfg";

        public static void Run()
        {
            if (!SqlRunner.IsAvailable)
            {
                Console.WriteLine("[bdeconfigcomparetest] SqlRunnerExePath not configured; aborting.");
                return;
            }

            if (!File.Exists(OriginalSettingsCfgPath) || !File.Exists(LargerBlockSizeCfgPath))
            {
                Console.WriteLine("[bdeconfigcomparetest] Missing one or both backup cfg files; aborting.");
                Console.WriteLine("  expected: {0}", OriginalSettingsCfgPath);
                Console.WriteLine("  expected: {0}", LargerBlockSizeCfgPath);
                return;
            }

            Directory.CreateDirectory(RootDir);

            // Preserve whatever is currently live so we can restore it exactly, regardless
            // of which backup it happens to match.
            byte[] preRunLiveCfgBytes = File.ReadAllBytes(LiveCfgPath);

            try
            {
                var cases = HeaderCompareTest.BuildCases();
                var summary = new List<string>();

                string v5RootDir = Path.Combine(RootDir, "v5_2048");
                string v7RootDir = Path.Combine(RootDir, "v7_32768");

                Console.WriteLine("=== Pass 1: idapi32.originalsettings.cfg (Paradox level 5, 2048-byte blocks) ===");
                SwapCfg(OriginalSettingsCfgPath);
                BuildAllCases(cases, v5RootDir);

                Console.WriteLine();
                Console.WriteLine("=== Pass 2: Idapi32.largerpdoxblocksize.cfg (Paradox level 7, 32768-byte blocks) ===");
                SwapCfg(LargerBlockSizeCfgPath);
                BuildAllCases(cases, v7RootDir);

                Console.WriteLine();
                Console.WriteLine("======================================================================");
                Console.WriteLine("Header diff: v5/2048 (ours-baseline) vs v7/32768, per case");
                Console.WriteLine("======================================================================");

                foreach (var c in cases)
                {
                    string baseName = c.Name.ToUpperInvariant();
                    if (baseName.Length > 8) baseName = baseName.Substring(0, 8);

                    string v5Dir = Path.Combine(v5RootDir, c.Name);
                    string v7Dir = Path.Combine(v7RootDir, c.Name);

                    if (!Directory.Exists(v5Dir) || !Directory.Exists(v7Dir))
                    {
                        Console.WriteLine();
                        Console.WriteLine("Case: {0}: SKIP (creation failed on one or both sides)", c.Name);
                        summary.Add(c.Name + ": SKIP(create-failed)");
                        continue;
                    }

                    Console.WriteLine();
                    Console.WriteLine("--- Case: {0} ---", c.Name);
                    int diffCount = HeaderCompareTest.CompareAllHeaders(v5Dir, v7Dir, baseName);
                    summary.Add(string.Format("{0}: diffs={1}", c.Name, diffCount));
                }

                Console.WriteLine();
                Console.WriteLine("======================================================================");
                Console.WriteLine("SUMMARY");
                Console.WriteLine("======================================================================");
                foreach (var line in summary)
                    Console.WriteLine("  " + line);
            }
            finally
            {
                // Restore whatever was live before this run, regardless of pass ordering above.
                File.WriteAllBytes(LiveCfgPath, preRunLiveCfgBytes);
                SqlRunner.EnsureNoStrayProcesses();
                Console.WriteLine();
                Console.WriteLine("[bdeconfigcomparetest] Restored original live idapi32.cfg.");
            }
        }

        /// <summary>
        /// Copies <paramref name="sourceCfgPath"/> over the live idapi32.cfg and force-kills
        /// any stray SQLRunner/BDE process so the next invocation picks up the new config
        /// (BDE reads idapi32.cfg at engine-init time, i.e. per SQLRunner process launch).
        /// </summary>
        private static void SwapCfg(string sourceCfgPath)
        {
            SqlRunner.EnsureNoStrayProcesses();
            File.Copy(sourceCfgPath, LiveCfgPath, overwrite: true);
        }

        private static void BuildAllCases(List<HeaderCompareTest.CaseDefinition> cases, string passRootDir)
        {
            foreach (var c in cases)
            {
                string caseDir = Path.Combine(passRootDir, c.Name);
                HeaderCompareTest.ResetDir(caseDir);

                string baseName = c.Name.ToUpperInvariant();
                if (baseName.Length > 8) baseName = baseName.Substring(0, 8);

                string dbPath = Path.Combine(caseDir, baseName + ".DB");
                foreach (var ddl in c.SqlRunnerDdl)
                {
                    HeaderCompareTest.RunSqlRunner(ddl.Replace("{PATH}", dbPath));
                }

                if (!File.Exists(dbPath))
                {
                    Console.WriteLine("  [{0}] FAIL: SQLRunner did not create {1}", c.Name, dbPath);
                    try { Directory.Delete(caseDir, recursive: true); } catch { /* best effort */ }
                    continue;
                }

                Console.WriteLine("  [{0}] created OK", c.Name);
            }
        }
    }
}
