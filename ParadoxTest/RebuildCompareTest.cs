using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Rebuild-focused counterpart to <see cref="HeaderCompareTest"/>: for the
    /// schema cases defined in <see cref="HeaderCompareTest.BuildCases"/>
    /// (NOIDX, PKONLY, PKALPHA, PKALPHIDX, AUTOALPIDX, PKALPMEMO, PKALPBLOB,
    /// AUTOALPMEMO, AUTOALPBLOB; AUTOPK was scrapped - see BuildCases()
    /// remarks), builds each table fresh via SQLRunner (real BDE), then
    /// produces six datasets per case:
    ///
    ///   orig_nodata      - pristine SQLRunner-created table, no rows
    ///   orig_1row        - pristine SQLRunner-created table, one row inserted
    ///   pdxrbld_nodata   - orig_nodata rebuilt in place by BDE's Pdxrbld.exe
    ///   pdxrbld_1row     - orig_1row rebuilt in place by BDE's Pdxrbld.exe
    ///   ourrebuild_nodata- orig_nodata rebuilt in place by TableRebuilder.Rebuild
    ///   ourrebuild_1row  - orig_1row rebuilt in place by TableRebuilder.Rebuild
    ///
    /// and reports byte/header diffs across every meaningful pairing (orig vs
    /// pdxrbld, orig vs ourrebuild, pdxrbld vs ourrebuild) for both the
    /// no-data and one-row states, plus a SQLRunner "select count(*)" oracle
    /// read against every dataset to confirm it's still functionally readable
    /// by real BDE.
    ///
    /// Note on the oracle's expected row count: SQLRunner's SELECT support
    /// doesn't return a real aggregate - it dumps one line per underlying
    /// record. Empirically, even a *zero-record* table still yields exactly
    /// one "Read 1 rows." line back from SQLRunner (rather than "Read 0
    /// rows."), so the "nodata" datasets are validated against an expected
    /// count of 1, same as the "1row" datasets - the oracle is really just
    /// confirming SQLRunner can open/scan the table at all, not the true
    /// record count.
    ///
    /// This is purely diagnostic (prints reports); the goal is to see
    /// specifically what our rebuild does differently from Pdxrbld (assumed
    /// consistently correct per Paradox7/BDE) and from an untouched original.
    /// </summary>
    internal static class RebuildCompareTest
    {
        private const string RootDir = @"c:\temp\rebuildcompare";

        public static void Run()
        {
            if (!SqlRunner.IsAvailable)
            {
                Console.WriteLine("[rebuildcomparetest] SqlRunnerExePath not configured; aborting.");
                return;
            }

            bool pdxrbldAvailable = Pdxrbld.IsAvailable;
            if (!pdxrbldAvailable)
            {
                Console.WriteLine("[rebuildcomparetest] PdxrbldExePath not configured/found; pdxrbld_* datasets will be skipped.");
            }

            Directory.CreateDirectory(RootDir);

            var cases = HeaderCompareTest.BuildCases();
            var summary = new List<string>();

            foreach (var c in cases)
            {
                Console.WriteLine();
                Console.WriteLine("======================================================================");
                Console.WriteLine("Case: {0}", c.Name);
                Console.WriteLine("======================================================================");

                string caseDir = Path.Combine(RootDir, c.Name);
                string baseName = c.Name.ToUpperInvariant();
                if (baseName.Length > 8) baseName = baseName.Substring(0, 8); // Paradox 8.3 table-name limit

                string origNoDataDir = Path.Combine(caseDir, "orig_nodata");
                string orig1RowDir = Path.Combine(caseDir, "orig_1row");
                string orig3RowDir = Path.Combine(caseDir, "orig_3row");
                string pdxNoDataDir = Path.Combine(caseDir, "pdxrbld_nodata");
                string pdx1RowDir = Path.Combine(caseDir, "pdxrbld_1row");
                string pdx3RowDir = Path.Combine(caseDir, "pdxrbld_3row");
                string ourNoDataDir = Path.Combine(caseDir, "ourrebuild_nodata");
                string our1RowDir = Path.Combine(caseDir, "ourrebuild_1row");
                string our3RowDir = Path.Combine(caseDir, "ourrebuild_3row");

                ResetDir(origNoDataDir);
                ResetDir(orig1RowDir);
                ResetDir(orig3RowDir);
                ResetDir(pdxNoDataDir);
                ResetDir(pdx1RowDir);
                ResetDir(pdx3RowDir);
                ResetDir(ourNoDataDir);
                ResetDir(our1RowDir);
                ResetDir(our3RowDir);

                // ---- 1. Build pristine SQLRunner original (no data) ----
                string origDbFileName = baseName + ".DB";
                string origNoDataDbPath = Path.Combine(origNoDataDir, origDbFileName);
                foreach (var ddl in c.SqlRunnerDdl)
                {
                    RunSqlRunner(ddl.Replace("{PATH}", origNoDataDbPath));
                }

                if (!File.Exists(origNoDataDbPath))
                {
                    Console.WriteLine("  [FAIL] SQLRunner did not create {0}; skipping this case.", origNoDataDbPath);
                    summary.Add(c.Name + ": FAIL(sqlrunner-create)");
                    continue;
                }

                // ---- 2. Build pristine SQLRunner original + 1 row ----
                CopyAllTableFiles(origNoDataDir, orig1RowDir, baseName);
                string orig1RowDbPath = Path.Combine(orig1RowDir, origDbFileName);
                RunSqlRunner(c.InsertSqlRunner.Replace("{PATH}", orig1RowDbPath));

                // ---- 2b. Build pristine SQLRunner original + 3 rows ----
                // A genuine multi-row (N>1) dataset is required to expose the
                // "select count(*) dumps one row per record instead of a
                // single aggregate row" failure mode: with only 0 or 1 real
                // records, a broken per-record fallback is indistinguishable
                // from a correct aggregate (both emit exactly one output row).
                CopyAllTableFiles(origNoDataDir, orig3RowDir, baseName);
                string orig3RowDbPath = Path.Combine(orig3RowDir, origDbFileName);
                var rowGen = c.InsertSqlRunnerForRow ?? (i => c.InsertSqlRunner);
                for (int i = 0; i < 3; i++)
                {
                    RunSqlRunner(rowGen(i).Replace("{PATH}", orig3RowDbPath));
                }

                // ---- 3. pdxrbld: nodata + 1row + 3row (in-place rebuild of copies) ----
                bool pdxNoDataOk = false, pdx1RowOk = false, pdx3RowOk = false;
                if (pdxrbldAvailable)
                {
                    CopyAllTableFiles(origNoDataDir, pdxNoDataDir, baseName);
                    pdxNoDataOk = RunPdxrbld(pdxNoDataDir, origDbFileName);

                    CopyAllTableFiles(orig1RowDir, pdx1RowDir, baseName);
                    pdx1RowOk = RunPdxrbld(pdx1RowDir, origDbFileName);

                    CopyAllTableFiles(orig3RowDir, pdx3RowDir, baseName);
                    pdx3RowOk = RunPdxrbld(pdx3RowDir, origDbFileName);
                }

                // ---- 4. our rebuild: nodata + 1row + 3row (in-place rebuild of copies) ----
                CopyAllTableFiles(origNoDataDir, ourNoDataDir, baseName);
                string ourNoDataDbPath = Path.Combine(ourNoDataDir, origDbFileName);
                bool ourNoDataOk = RunOurRebuild(ourNoDataDbPath);

                CopyAllTableFiles(orig1RowDir, our1RowDir, baseName);
                string our1RowDbPath = Path.Combine(our1RowDir, origDbFileName);
                bool our1RowOk = RunOurRebuild(our1RowDbPath);

                CopyAllTableFiles(orig3RowDir, our3RowDir, baseName);
                string our3RowDbPath = Path.Combine(our3RowDir, origDbFileName);
                bool our3RowOk = RunOurRebuild(our3RowDbPath);

                // ---- 5. SQLRunner "select count(*)" oracle against all datasets ----
                // SQLRunner now returns a genuine aggregate: exactly one
                // "Read 1 rows." line (always 1, even for an empty table -
                // the aggregate result itself is still "1 row"), plus a
                // "Count: N" line with the real record count. Both are
                // checked: reading more than 1 row indicates a broken
                // fallback to per-record dumping, and Count must match the
                // dataset's actual record count (0/1/3).
                Console.WriteLine("--- SQLRunner oracle: select count(*) ---");
                bool oracleOrigNoData = RunCountOracle(origNoDataDbPath, "orig_nodata", 0);
                bool oracleOrig1Row = RunCountOracle(orig1RowDbPath, "orig_1row", 1);
                bool oracleOrig3Row = RunCountOracle(orig3RowDbPath, "orig_3row", 3);
                bool oraclePdxNoData = pdxrbldAvailable && pdxNoDataOk && RunCountOracle(Path.Combine(pdxNoDataDir, origDbFileName), "pdxrbld_nodata", 0);
                bool oraclePdx1Row = pdxrbldAvailable && pdx1RowOk && RunCountOracle(Path.Combine(pdx1RowDir, origDbFileName), "pdxrbld_1row", 1);
                bool oraclePdx3Row = pdxrbldAvailable && pdx3RowOk && RunCountOracle(Path.Combine(pdx3RowDir, origDbFileName), "pdxrbld_3row", 3);
                bool oracleOurNoData = ourNoDataOk && RunCountOracle(ourNoDataDbPath, "ourrebuild_nodata", 0);
                bool oracleOur1Row = our1RowOk && RunCountOracle(our1RowDbPath, "ourrebuild_1row", 1);
                bool oracleOur3Row = our3RowOk && RunCountOracle(our3RowDbPath, "ourrebuild_3row", 3);

                // ---- 6. Header/byte diffs across pairings ----
                Console.WriteLine("--- Header comparison: orig_nodata vs ourrebuild_nodata ---");
                int diffOurNoData = CompareAllHeaders(origNoDataDir, ourNoDataDir, baseName);

                Console.WriteLine("--- Header comparison: orig_1row vs ourrebuild_1row ---");
                int diffOur1Row = CompareAllHeaders(orig1RowDir, our1RowDir, baseName);

                Console.WriteLine("--- Header comparison: orig_3row vs ourrebuild_3row ---");
                int diffOur3Row = CompareAllHeaders(orig3RowDir, our3RowDir, baseName);

                int diffPdxNoData = -1, diffPdx1Row = -1, diffPdx3Row = -1, diffOurVsPdxNoData = -1, diffOurVsPdx1Row = -1, diffOurVsPdx3Row = -1;
                if (pdxrbldAvailable)
                {
                    if (pdxNoDataOk)
                    {
                        Console.WriteLine("--- Header comparison: orig_nodata vs pdxrbld_nodata ---");
                        diffPdxNoData = CompareAllHeaders(origNoDataDir, pdxNoDataDir, baseName);

                        Console.WriteLine("--- Header comparison: pdxrbld_nodata vs ourrebuild_nodata ---");
                        diffOurVsPdxNoData = CompareAllHeaders(pdxNoDataDir, ourNoDataDir, baseName);
                    }
                    if (pdx1RowOk)
                    {
                        Console.WriteLine("--- Header comparison: orig_1row vs pdxrbld_1row ---");
                        diffPdx1Row = CompareAllHeaders(orig1RowDir, pdx1RowDir, baseName);

                        Console.WriteLine("--- Header comparison: pdxrbld_1row vs ourrebuild_1row ---");
                        diffOurVsPdx1Row = CompareAllHeaders(pdx1RowDir, our1RowDir, baseName);
                    }
                    if (pdx3RowOk)
                    {
                        Console.WriteLine("--- Header comparison: orig_3row vs pdxrbld_3row ---");
                        diffPdx3Row = CompareAllHeaders(orig3RowDir, pdx3RowDir, baseName);

                        Console.WriteLine("--- Header comparison: pdxrbld_3row vs ourrebuild_3row ---");
                        diffOurVsPdx3Row = CompareAllHeaders(pdx3RowDir, our3RowDir, baseName);
                    }
                }

                summary.Add(string.Format(
                    "{0}: oracle[orig0={1},orig1={2},orig3={3},pdx0={4},pdx1={5},pdx3={6},our0={7},our1={8},our3={9}] " +
                    "diffs[orig-our0={10},orig-our1={11},orig-our3={12},orig-pdx0={13},orig-pdx1={14},orig-pdx3={15},pdx-our0={16},pdx-our1={17},pdx-our3={18}]",
                    c.Name,
                    B(oracleOrigNoData), B(oracleOrig1Row), B(oracleOrig3Row), B(oraclePdxNoData), B(oraclePdx1Row), B(oraclePdx3Row), B(oracleOurNoData), B(oracleOur1Row), B(oracleOur3Row),
                    diffOurNoData, diffOur1Row, diffOur3Row, diffPdxNoData, diffPdx1Row, diffPdx3Row, diffOurVsPdxNoData, diffOurVsPdx1Row, diffOurVsPdx3Row));
            }

            Console.WriteLine();
            Console.WriteLine("======================================================================");
            Console.WriteLine("SUMMARY (oracle: PASS/FAIL per dataset; diffs: header byte-diff count, -1 = skipped)");
            Console.WriteLine("======================================================================");
            foreach (var line in summary)
                Console.WriteLine("  " + line);
        }

        private static string B(bool ok) => ok ? "PASS" : "FAIL";

        /// <summary>
        /// Standalone oracle check: runs SQLRunner's "select count(*)"
        /// against a single table path, for quickly confirming/reproducing
        /// a reported rebuild failure without running the full six-case
        /// comparison matrix. Pass expectedRecordCount = -1 to just print
        /// whatever SQLRunner reports, without a PASS/FAIL verdict.
        /// </summary>
        public static void CheckCount(string dbPath, int expectedRecordCount)
        {
            if (!SqlRunner.IsAvailable)
            {
                Console.WriteLine("[checkrebuildcount] SqlRunnerExePath not configured; aborting.");
                return;
            }

            if (expectedRecordCount < 0)
            {
                RunSqlRunnerCapture("select count(*) from '" + dbPath + "'", out string stdout, out _);
                var readMatch = Regex.Match(stdout, @"Read (\d+) rows?\.", RegexOptions.IgnoreCase);
                var countMatch = Regex.Match(stdout, @"Count:\s*(-?\d+)", RegexOptions.IgnoreCase);
                if (readMatch.Success && countMatch.Success)
                    Console.WriteLine("SQLRunner reports {0} row(s) read, Count={1}.", readMatch.Groups[1].Value, countMatch.Groups[1].Value);
                else
                    Console.WriteLine("Could not parse \"Read N rows.\" / \"Count: N\" lines from SQLRunner output.");
                return;
            }

            RunCountOracle(dbPath, Path.GetFileName(dbPath), expectedRecordCount);
        }

        // --------------------------------------------------------------
        // Rebuild invocations
        // --------------------------------------------------------------

        /// <summary>
        /// Invokes BDE's Pdxrbld.exe in silent/unattended mode against the
        /// given table:
        ///   /F&lt;databasedir&gt; (no space/equals)
        ///   /T&lt;tablefilename.db&gt; (no space/equals)
        ///   -R2  rebuild with verify check
        ///   -P+  pack (compact) the table
        ///   -L+  log to file
        ///   -Q+  quiet/silent mode
        /// Confirms success by checking Pdxrbld's own append-only log file
        /// for a "no errors found" entry for this table, appended after this
        /// invocation started.
        /// </summary>
        private static bool RunPdxrbld(string tableDir, string tableFileName) => Pdxrbld.Rebuild(tableDir, tableFileName);

        /// <summary>
        /// Invokes ParadoxReader.TableRebuilder.Rebuild against the given
        /// .DB path, reporting any exception rather than letting it crash
        /// the whole harness run.
        /// </summary>
        private static bool RunOurRebuild(string dbPath)
        {
            Console.WriteLine("TableRebuilder.Rebuild> {0}", dbPath);
            try
            {
                var result = TableRebuilder.Rebuild(dbPath);
                Console.WriteLine("  Rebuilt {0}: {1} record(s) migrated.", dbPath, result.RecordsMigrated);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] TableRebuilder.Rebuild threw: {0}", ex);
                return false;
            }
        }

        /// <summary>
        /// Confirms functional readability AND correctness via SQLRunner's
        /// "select count(*)" oracle. SQLRunner now returns a genuine
        /// aggregate: exactly one "Read 1 rows." line (always 1, regardless
        /// of the table's actual record count - even an empty table is
        /// still "1 result"), followed by a "Count: N" line where N is the
        /// real record count. Both are validated: reading anything other
        /// than "Read 1 rows." indicates SQLRunner fell back to dumping one
        /// line per record (a broken/corrupt table), and the parsed Count
        /// must match <paramref name="expectedRecordCount"/>.
        /// </summary>
        private static bool RunCountOracle(string dbPath, string label, int expectedRecordCount)
        {
            if (!File.Exists(dbPath))
            {
                Console.WriteLine("  [{0}] SKIP (file not found: {1})", label, dbPath);
                return false;
            }

            bool exited = RunSqlRunnerCapture("select count(*) from '" + dbPath + "'", out string stdout, out string stderr);

            var readMatch = Regex.Match(stdout, @"Read (\d+) rows?\.", RegexOptions.IgnoreCase);
            var countMatch = Regex.Match(stdout, @"Count:\s*(-?\d+)", RegexOptions.IgnoreCase);

            if (!exited || !readMatch.Success || !countMatch.Success)
            {
                Console.WriteLine("  [{0}] FAIL (could not parse \"Read N rows.\" / \"Count: N\" lines from SQLRunner output)", label);
                Console.WriteLine("  ---- raw stdout ----");
                Console.WriteLine(stdout);
                Console.WriteLine("  ---- raw stderr ----");
                Console.WriteLine(stderr);
                Console.WriteLine("  ---------------------");
                return false;
            }

            int readRows = int.Parse(readMatch.Groups[1].Value);
            int actualCount = int.Parse(countMatch.Groups[1].Value);

            bool readOk = readRows == 1; // count(*) must always be a single aggregate result
            bool countOk = actualCount == expectedRecordCount;
            bool ok = readOk && countOk;

            Console.WriteLine("  [{0}] {1} (read {2} row(s) [expected 1], Count={3} [expected {4}])",
                label, ok ? "PASS" : "FAIL", readRows, actualCount, expectedRecordCount);
            return ok;
        }

        // --------------------------------------------------------------
        // File helpers
        // --------------------------------------------------------------

        private static void ResetDir(string dir)
        {
            if (Directory.Exists(dir))
            {
                foreach (var f in Directory.GetFiles(dir))
                {
                    try { File.Delete(f); } catch { /* best effort */ }
                }
            }
            else
            {
                Directory.CreateDirectory(dir);
            }
        }

        /// <summary>
        /// Copies every file sharing <paramref name="baseName"/> (case
        /// insensitively) from <paramref name="srcDir"/> into
        /// <paramref name="destDir"/>, e.g. .DB/.MB/.PX/.XGn/.YGn.
        /// </summary>
        private static void CopyAllTableFiles(string srcDir, string destDir, string baseName)
        {
            Directory.CreateDirectory(destDir);
            foreach (var f in Directory.GetFiles(srcDir, baseName + ".*"))
            {
                string destPath = Path.Combine(destDir, Path.GetFileName(f));
                File.Copy(f, destPath, true);
            }
        }

        // --------------------------------------------------------------
        // Header comparison (mirrors HeaderCompareTest.CompareAllHeaders)
        // --------------------------------------------------------------

        private static int CompareAllHeaders(string dirA, string dirB, string baseName)
        {
            var aFiles = Directory.GetFiles(dirA, baseName + ".*")
                .Where(f => !Path.GetExtension(f).Equals(".LCK", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(f => Path.GetExtension(f).ToUpperInvariant(), f => f);
            var bFiles = Directory.GetFiles(dirB, baseName + ".*")
                .Where(f => !Path.GetExtension(f).Equals(".LCK", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(f => Path.GetExtension(f).ToUpperInvariant(), f => f);

            var allExtensions = aFiles.Keys.Union(bFiles.Keys).OrderBy(e => e).ToList();
            int totalDiffs = 0;

            foreach (var ext in allExtensions)
            {
                if (ext.Equals(".MB", StringComparison.OrdinalIgnoreCase))
                    continue; // blob files have no comparable "header" in the same sense

                if (!aFiles.ContainsKey(ext))
                {
                    Console.WriteLine("  [{0}] MISSING on A side", ext);
                    totalDiffs++;
                    continue;
                }
                if (!bFiles.ContainsKey(ext))
                {
                    Console.WriteLine("  [{0}] MISSING on B side", ext);
                    totalDiffs++;
                    continue;
                }

                totalDiffs += CompareHeaderPair(ext, aFiles[ext], bFiles[ext]);
            }

            if (totalDiffs == 0)
                Console.WriteLine("  All paired headers byte-identical.");

            return totalDiffs;
        }

        private static int CompareHeaderPair(string ext, string aPath, string bPath)
        {
            byte[] aHeader = ReadHeaderRegion(aPath);
            byte[] bHeader = ReadHeaderRegion(bPath);

            int minLen = Math.Min(aHeader.Length, bHeader.Length);
            int maxLen = Math.Max(aHeader.Length, bHeader.Length);
            int diffCount = 0;

            if (aHeader.Length != bHeader.Length)
            {
                Console.WriteLine("  [{0}] HeaderSize differs: a={1} b={2}", ext, aHeader.Length, bHeader.Length);
                diffCount++;
            }

            for (int i = 0; i < minLen; i++)
            {
                if (aHeader[i] != bHeader[i])
                {
                    string fieldName = DescribeOffset(i);
                    Console.WriteLine("  [{0}] offset 0x{1:X2}{2}: a=0x{3:X2} b=0x{4:X2}",
                        ext, i, fieldName != null ? " (" + fieldName + ")" : "", aHeader[i], bHeader[i]);
                    diffCount++;
                }
            }

            if (maxLen > minLen)
            {
                Console.WriteLine("  [{0}] {1} extra trailing byte(s) beyond the shorter header not compared.", ext, maxLen - minLen);
            }

            return diffCount;
        }

        private static byte[] ReadHeaderRegion(string path)
        {
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var r = new BinaryReader(fs))
            {
                if (fs.Length < 4)
                    return r.ReadBytes((int)fs.Length);

                fs.Position = 0x02;
                ushort headerSize = r.ReadUInt16();
                int len = headerSize > 0 && headerSize <= fs.Length ? headerSize : (int)fs.Length;

                fs.Position = 0;
                return r.ReadBytes(len);
            }
        }

        private static string DescribeOffset(int offset)
        {
            if (offset == ParadoxHeaderOffsets.RecordSize) return "RecordSize";
            if (offset == ParadoxHeaderOffsets.HeaderSize) return "HeaderSize";
            if (offset == ParadoxHeaderOffsets.FileType) return "FileType";
            if (offset == ParadoxHeaderOffsets.MaxTableSize) return "MaxTableSize";
            if (offset == ParadoxHeaderOffsets.RecordCount) return "RecordCount";
            if (offset >= ParadoxHeaderOffsets.BlockChain && offset < ParadoxHeaderOffsets.BlockChain + 8) return "BlockChain";
            if (offset == ParadoxHeaderOffsets.PxRootBlockId || offset == ParadoxHeaderOffsets.PxRootBlockId + 1) return "PxRootBlockId";
            if (offset == ParadoxHeaderOffsets.PxLevelCount) return "PxLevelCount";
            if (offset == ParadoxHeaderOffsets.WriteCounter) return "WriteCounter";
            if (offset == ParadoxHeaderOffsets.ChangeCount1) return "ChangeCount1";
            if (offset == ParadoxHeaderOffsets.ChangeCount2) return "ChangeCount2";
            if (offset >= ParadoxHeaderOffsets.MaxBlocks && offset < ParadoxHeaderOffsets.MaxBlocks + 2) return "MaxBlocks";
            if (offset >= ParadoxHeaderOffsets.AutoIncVal && offset < ParadoxHeaderOffsets.AutoIncVal + 4) return "AutoIncVal";
            if (offset >= ParadoxHeaderOffsets.ChangeCount4 && offset < ParadoxHeaderOffsets.ChangeCount4 + 2) return "ChangeCount4";
            if (offset == ParadoxHeaderOffsets.HasBlobFlag) return "HasBlobFlag";
            return null;
        }

        // --------------------------------------------------------------
        // SQLRunner process invocation
        // --------------------------------------------------------------

        private static void RunSqlRunner(string sql)
        {
            RunSqlRunnerCapture(sql, out _, out _);
        }

        // "select count(*)" over a blob-bearing table can take noticeably
        // longer than a trivial table, so give SQLRunner extra time to
        // compute/print its result before feeding the dismiss-prompt ENTER
        // keystroke, and a longer overall timeout before treating it as hung.
        private static bool RunSqlRunnerCapture(string sql, out string stdout, out string stderr)
        {
            var result = SqlRunner.Execute(sql, preStdinDelayMs: 15000, timeoutMs: 30000);
            stdout = result.Stdout;
            stderr = result.Stderr;
            return result.Exited;
        }
    }
}
