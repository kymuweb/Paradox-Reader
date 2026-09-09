using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Focused regression harness for ParadoxReader's table *creation* logic
    /// (<see cref="TableCreator"/> / <see cref="ParadoxHeaderBuilder"/>), the
    /// area that to date has been the hardest to get byte-for-byte correct.
    ///
    /// Reuses the same schema shapes as <see cref="HeaderCompareTest"/> (via
    /// <see cref="HeaderCompareTest.BuildCases"/>), but instead of diffing
    /// only the header region, this does a *full* byte comparison of every
    /// created file, at two points:
    ///
    ///  1. Immediately after creation, before any data exists (both sides
    ///     blank/empty tables) - this isolates whether our CREATE logic alone
    ///     produces the same on-disk bytes as real BDE (via SQLRunner CREATE
    ///     TABLE/CREATE INDEX).
    ///  2. After inserting one equivalent record into *both* sides using our
    ///     own <see cref="ParadoxTableFile.InsertRecord"/> logic (not
    ///     SQLRunner INSERT on the SQLRunner-created side). This is
    ///     deliberate: using the same insert path on both sides minimizes the
    ///     number of moving parts under test (isolating create-time
    ///     differences rather than mixing in insert-time differences), and
    ///     is also a practical necessity - if our CREATE logic has produced a
    ///     subtly corrupt file, SQLRunner/BDE can refuse to open or insert
    ///     into it, which would prevent step 2 from running at all on a
    ///     BDE-created table's twin.
    ///
    /// This is purely diagnostic (prints reports); it does not fail the
    /// process on a diff, since some diffs are expected/benign (e.g. table
    /// name bytes embedded in the header) - the point is visibility into
    /// exactly what differs, to guide a fix to the creation path.
    /// </summary>
    internal static class CreateTableCompareTest
    {
        private const string RootDir = @"c:\temp\createtablecompare";

        public static void Run()
        {
            if (!SqlRunner.IsAvailable)
            {
                Console.WriteLine("[createtabletest] SQLRunner not found at {0}; aborting.", SqlRunner.ExePath);
                return;
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
                string oursDir = Path.Combine(caseDir, "ours");
                string sqlDir = Path.Combine(caseDir, "sqlrunner");
                ResetDir(oursDir);
                ResetDir(sqlDir);

                string baseName = c.Name.ToUpperInvariant();
                if (baseName.Length > 8) baseName = baseName.Substring(0, 8); // Paradox 8.3 table-name limit

                // ---- Create "ours" (ParadoxReader TableCreator) ----
                string oursDbPath = Path.Combine(oursDir, baseName + ".DB");
                var oursSchema = HeaderCompareTest.CloneSchemaWithName(c.Schema, baseName);
                TableCreator.CreateNew(oursDbPath, oursSchema);

                // ---- Create "sqlrunner" (real BDE via SQLRunner DDL) ----
                string sqlDbPath = Path.Combine(sqlDir, baseName + ".DB");
                foreach (var ddl in c.SqlRunnerDdl)
                {
                    RunSqlRunner(ddl.Replace("{PATH}", sqlDbPath));
                }

                if (!File.Exists(sqlDbPath))
                {
                    Console.WriteLine("  [FAIL] SQLRunner did not create {0}; skipping this case.", sqlDbPath);
                    summary.Add(c.Name + ": FAIL(sqlrunner-create)");
                    continue;
                }

                Console.WriteLine("--- Full byte comparison BLANK (immediately after create) ---");
                int blankDiffCount = CompareAllFilesFullByte(oursDir, sqlDir, baseName);

                Console.WriteLine("--- SQLRunner count(*) oracle BLANK (immediately after create) ---");
                bool oursCountOkBlank = SqlRunner.CountOracle(oursDbPath, "ours", 0);
                bool sqlCountOkBlank = SqlRunner.CountOracle(sqlDbPath, "sqlrunner", 0);

                // ---- Insert one record into BOTH sides using OUR insert
                // logic, so the SQLRunner-created table isn't touched by
                // SQLRunner itself (also required if the SQLRunner-created
                // table happens to be subtly incompatible with our writer,
                // that's itself useful signal - but the point here is to
                // hold the insert path constant across both sides). ----
                bool oursInsertOk = TryInsert(oursDbPath, c.InsertOurs, "ours");
                bool sqlInsertOk = TryInsert(sqlDbPath, c.InsertOurs, "sqlrunner-created");

                if (!oursInsertOk || !sqlInsertOk)
                {
                    summary.Add(string.Format("{0}: blank-diffs={1}, post-insert=SKIPPED(insert-failed)", c.Name, blankDiffCount));
                    continue;
                }

                Console.WriteLine("--- Full byte comparison AFTER ONE INSERT (both via our InsertRecord) ---");
                int insertDiffCount = CompareAllFilesFullByte(oursDir, sqlDir, baseName);

                Console.WriteLine("--- SQLRunner count(*) oracle AFTER ONE INSERT ---");
                bool oursCountOkInsert = SqlRunner.CountOracle(oursDbPath, "ours", 1);
                bool sqlCountOkInsert = SqlRunner.CountOracle(sqlDbPath, "sqlrunner", 1);

                summary.Add(string.Format(
                    "{0}: blank-diffs={1}, post-insert-diffs={2}, count-oracle(blank ours/sql)={3}/{4}, count-oracle(insert ours/sql)={5}/{6}",
                    c.Name, blankDiffCount, insertDiffCount,
                    oursCountOkBlank ? "PASS" : "FAIL", sqlCountOkBlank ? "PASS" : "FAIL",
                    oursCountOkInsert ? "PASS" : "FAIL", sqlCountOkInsert ? "PASS" : "FAIL"));
            }

            Console.WriteLine();
            Console.WriteLine("======================================================================");
            Console.WriteLine("SUMMARY");
            Console.WriteLine("======================================================================");
            foreach (var line in summary)
                Console.WriteLine("  " + line);
        }

        /// <summary>
        /// Inserts <paramref name="fieldValues"/> into the table at
        /// <paramref name="dbPath"/> using our own <see cref="ParadoxTableFile.InsertRecord"/>,
        /// catching and reporting any exception rather than letting it abort
        /// the whole case (a create-time corruption on either side is
        /// exactly the kind of thing this harness is meant to surface).
        /// </summary>
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

        // --------------------------------------------------------------
        // Full-file byte comparison (deliberately distinct from
        // HeaderCompareTest.CompareAllHeaders, which only compares each
        // file's declared header region - this compares entire files,
        // including record/index data, since after an insert the data
        // written beyond the header is exactly what we want visibility
        // into as well).
        // --------------------------------------------------------------

        private static int CompareAllFilesFullByte(string oursDir, string sqlDir, string baseName)
        {
            var oursFiles = Directory.GetFiles(oursDir, baseName + ".*")
                .Where(f => !Path.GetExtension(f).Equals(".LCK", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(f => Path.GetExtension(f).ToUpperInvariant(), f => f);
            var sqlFiles = Directory.GetFiles(sqlDir, baseName + ".*")
                .Where(f => !Path.GetExtension(f).Equals(".LCK", StringComparison.OrdinalIgnoreCase))
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
                Console.WriteLine("  All paired files byte-identical (full file, not just header).");

            return totalDiffs;
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

        private static void RunSqlRunner(string sql) => SqlRunner.Execute(sql);
    }
}
