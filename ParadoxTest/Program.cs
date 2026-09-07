using System;
using System.Diagnostics;

namespace ParadoxTest
{
    /// <summary>
    /// Command-line entry point for the ParadoxTest harness. Dispatches to:
    /// - <see cref="CorpusTest"/> for the schema-agnostic, data-folder-driven
    ///   regression run (the default / "corpustest" mode).
    /// - <see cref="LibUpdateTest"/> for the dedicated library update/create
    ///   tests, via <see cref="MiscTests"/> aliases.
    /// - <see cref="MiscTests"/> for all other one-off diagnostic/test modes
    ///   (SQLRunner-only mode, harness-only mode, step comparison, PX index
    ///   growth, password tests, and the legacy TESTTAB.DB "suite").
    /// </summary>
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0] == "libupdatetest")
            {
                MiscTests.RunLibUpdateTest();
                return;
            }

            if (args.Length > 0 && args[0] == "libcreatetest")
            {
                MiscTests.RunLibCreateTest();
                return;
            }

            if (args.Length > 0 && args[0] == "pxpasswordtest")
            {
                MiscTests.RunPxPasswordTestMode();
                return;
            }

            if (args.Length > 0 && args[0] == "pxpasswordwritetest")
            {
                string writeTestPath = args.Length > 1 ? args[1] : @"C:\temp\pxpwtest\testtab_passworded_withdata.DB";
                MiscTests.RunPxPasswordWriteTestMode(writeTestPath);
                return;
            }

            if (args.Length > 0 && args[0] == "rebuildtest")
            {
                MiscTests.RunRebuildTestMode();
                return;
            }

            if (args.Length > 0 && args[0] == "freshrebuildtest")
            {
                // Usage: ParadoxTest.exe freshrebuildtest [insertCount]
                // Creates a brand-new table via SQLRunner (real BDE), rebuilds
                // it with TableRebuilder, and compares rebuilt vs. pristine to
                // isolate whether TableRebuilder itself corrupts an
                // otherwise-known-good table, independent of any pre-existing
                // corruption in a corpus table. Defaults to 0 records (empty
                // table); pass 1 to test the smallest non-empty case.
                int insertCount = args.Length > 1 && int.TryParse(args[1], out var ic) ? ic : 0;
                MiscTests.RunFreshRebuildTest(insertCount);
                return;
            }

            if (args.Length > 0 && args[0] == "indexoutofdatetest")
            {
                MiscTests.RunIndexOutOfDateTestMode();
                return;
            }

            if (args.Length > 0 && args[0] == "sqlenginetest")
            {
                SqlEngineTest.Run();
                return;
            }

            if ((args.Length > 0 && args[0] == "corpustest") || args.Length == 0)
            {
                // Usage: ParadoxTest.exe [corpustest] [dataRoot] [maxTables] [filter]
                // Schema-agnostic test mode: walks every table found in
                // dataRoot (default: .\data relative to the current working
                // directory, i.e. bin\Debug\data when run from Visual
                // Studio/Test Explorer or the exe's own folder, falling back
                // to the CorpusDataRootPath appSetting if set), infers each
                // table's schema from its own
                // header, and exercises append/update/read/lookup operations
                // against it, comparing against SQLRunner where available.
                // By default only a small random sample of tables is
                // processed (maxTables=12) so a full corpus (which can be
                // hundreds of tables) isn't scanned unintentionally; pass an
                // explicit maxTables (e.g. 0 for no limit) to override.
                string dataRoot = args.Length > 1 ? args[1] : null;
                int maxTables = args.Length > 2 && int.TryParse(args[2], out var mt) ? mt : 12;
                string filter = args.Length > 3 ? args[3] : null;
                Trace.Listeners.Add(new ConsoleTraceListener());
                CorpusTest.Run(dataRoot, maxTables, filter);
                return;
            }

            // Before doing anything else that might launch SQLRunner, make
            // sure we're starting from a genuinely clean state: no stray
            // SQLRunner process left over from a prior hung/killed run, and
            // no leftover *.LCK files. Without this, a previous crash can
            // silently leave a zombie SQLRunner (or lock) around that then
            // fights with the next run.
            MiscTests.EnsureCleanState();

            if (args.Length > 0 && args[0] == "sqlrunnermode")
            {
                MiscTests.RunSqlRunnerMode();
                return;
            }

            if (args.Length > 0 && args[0] == "harnessmode")
            {
                MiscTests.RunHarnessMode();
                return;
            }

            if (args.Length > 0 && args[0] == "comparesteps")
            {
                MiscTests.RunCompareStepsMode();
                return;
            }

            if (args.Length > 0 && args[0] == "comparerebuild")
            {
                // Usage: ParadoxTest.exe comparerebuild <dir> <baseNameA> <baseNameB>
                //     or ParadoxTest.exe comparerebuild <dirA> <baseNameA> <dirB> <baseNameB>
                // Byte-compares (and header-decodes) all files sharing a base
                // name (.DB/.MB/.PX/.XGn/.YGn) between two rebuilds, e.g. our
                // rebuild vs. the pdxrbld rebuild of the same source table:
                //   ParadoxTest.exe comparerebuild c:\temp\paradoxtest PatientBlobs_ourrebuild PatientBlobs_pdxrbldrebuild
                if (args.Length == 4)
                {
                    MiscTests.RunCompareRebuildMode(args[1], args[2], args[3]);
                }
                else if (args.Length == 5)
                {
                    MiscTests.RunCompareRebuildMode(args[1], args[2], args[3], args[4]);
                }
                else
                {
                    Console.WriteLine("Usage: ParadoxTest.exe comparerebuild <dir> <baseNameA> <baseNameB>");
                    Console.WriteLine("   or: ParadoxTest.exe comparerebuild <dirA> <baseNameA> <dirB> <baseNameB>");
                }
                return;
            }

            if (args.Length > 0 && args[0] == "growpxindex")
            {
                int targetCount = args.Length > 1 && int.TryParse(args[1], out var n) ? n : 5000;
                MiscTests.RunGrowPxIndexModePublic(targetCount);
                return;
            }

            if (args.Length > 0 && args[0] == "rebuildpath")
            {
                // Usage: ParadoxTest.exe rebuildpath <full path to .DB>
                // Runs TableRebuilder.Rebuild directly against an arbitrary
                // existing .DB file in-place (for ad hoc corpus verification).
                string dbPath = args[1];
                var result = ParadoxReader.TableRebuilder.Rebuild(dbPath);
                Console.WriteLine("Rebuilt {0}: {1} record(s) migrated.", dbPath, result.RecordsMigrated);
                return;
            }

            if (args.Length > 0 && args[0] == "suite")
            {
                // Usage: ParadoxTest.exe suite [TABLENAME.DB]
                // Runs the standard Test 1-6 suite against the given fixture
                // table (must exist in ParadoxTest\data alongside its
                // .PX/.MB/secondary index files). Defaults to TESTTAB.DB.
                string tableName = args.Length > 1 ? args[1] : null;
                MiscTests.RunSuiteMode(tableName);
                return;
            }
        }
    }
}
