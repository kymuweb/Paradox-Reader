using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Diagnostic harness that investigates whether ParadoxReader's table
    /// *creation* logic (<see cref="TableCreator"/> / <see cref="ParadoxHeaderBuilder"/>,
    /// and by extension <see cref="TableRebuilder.CreateEmptyTableSkeleton"/>'s
    /// approach of cloning-and-resetting an existing file's header) produces
    /// the same on-disk header bytes as a real BDE-created table (via
    /// SQLRunner CREATE TABLE/CREATE INDEX), for a range of schema shapes of
    /// increasing complexity.
    ///
    /// For each case this:
    ///  1. Builds the schema via ParadoxReader (TableCreator.CreateNew) in one
    ///     folder, and the equivalent schema via SQLRunner DDL in a sibling
    ///     folder.
    ///  2. Byte-diffs every header region (.DB, .PX, and any .Xnn/.Xgn/.Ynn/.Ygn
    ///     secondary index files) between the two, annotating any diff that
    ///     falls on a known <see cref="ParadoxHeaderOffsets"/> field.
    ///  3. Inserts one equivalent record into each side (ours via
    ///     ParadoxTableFile.InsertRecord, SQLRunner's via INSERT INTO) and
    ///     repeats the header diff.
    ///
    /// This is purely diagnostic (prints reports); it does not assert
    /// pass/fail in the sense of failing the process, since some diffs are
    /// expected/benign (e.g. table name bytes) - the point is visibility into
    /// exactly what differs, to guide a subsequent fix to the creation path.
    /// </summary>
    internal static class HeaderCompareTest
    {
        private const string RootDir = @"c:\temp\headercompare";

        internal class CaseDefinition
        {
            public string Name;
            public TableSchemaDefinition Schema;
            public List<string> SqlRunnerDdl; // CREATE TABLE + CREATE INDEX statements
            public string InsertSqlRunner;    // INSERT INTO statement for the post-insert pass
            public object[] InsertOurs;       // field values for ParadoxTableFile.InsertRecord

            /// <summary>
            /// Generates a distinct-row INSERT INTO statement (with "{PATH}"
            /// placeholder) for the given zero-based row index, used to build
            /// multi-row datasets (e.g. 3+ rows) without violating PRIMARY KEY
            /// uniqueness. Defaults to a variant of InsertSqlRunner if not set.
            /// </summary>
            public Func<int, string> InsertSqlRunnerForRow;
        }

        public static void Run()
        {
            if (!SqlRunner.IsAvailable)
            {
                Console.WriteLine("[headercomparetest] SqlRunnerExePath not configured; aborting.");
                return;
            }

            Directory.CreateDirectory(RootDir);

            var cases = BuildCases();
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

                // ---- Build "ours" ----
                string oursDbPath = Path.Combine(oursDir, baseName + ".DB");
                var oursSchema = CloneSchemaWithName(c.Schema, baseName);
                TableCreator.CreateNew(oursDbPath, oursSchema);

                // ---- Build "sqlrunner" ----
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

                Console.WriteLine("--- Header comparison AFTER CREATE (before any inserts) ---");
                int createDiffCount = CompareAllHeaders(oursDir, sqlDir, baseName);

                // ---- Insert one record into each side ----
                using (var t = new ParadoxTableFile(oursDbPath))
                    t.InsertRecord(c.InsertOurs);

                RunSqlRunner(c.InsertSqlRunner.Replace("{PATH}", sqlDbPath));

                Console.WriteLine("--- Header comparison AFTER ONE INSERT ---");
                int insertDiffCount = CompareAllHeaders(oursDir, sqlDir, baseName);

                summary.Add(string.Format("{0}: create-diffs={1}, post-insert-diffs={2}", c.Name, createDiffCount, insertDiffCount));
            }

            Console.WriteLine();
            Console.WriteLine("======================================================================");
            Console.WriteLine("SUMMARY");
            Console.WriteLine("======================================================================");
            foreach (var line in summary)
                Console.WriteLine("  " + line);
        }

        // --------------------------------------------------------------
        // Case definitions
        // --------------------------------------------------------------

        internal static List<CaseDefinition> BuildCases()
        {
            var cases = new List<CaseDefinition>();

            // 1. Plain INTEGER field, no primary key, no index at all.
            cases.Add(new CaseDefinition
            {
                Name = "NOIDX",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("VAL", ParadoxFieldTypes.Long, 4, false),
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (VAL INTEGER)"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (VAL) VALUES (1)",
                InsertOurs = new object[] { 1 },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (VAL) VALUES (" + (i + 1) + ")"
            });

            // 2. INTEGER primary key (plain .PX, no secondary index).
            cases.Add(new CaseDefinition
            {
                Name = "PKONLY",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (ID INTEGER, PRIMARY KEY (ID))"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (ID) VALUES (1)",
                InsertOurs = new object[] { 1 },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (ID) VALUES (" + (i + 1) + ")"
            });

            // 3. AUTOINC primary key.
            // NOTE: Scrapped - SQLRunner rejects "INSERT INTO ... (ID)
            // VALUES (0)" for AUTOINC columns ("Errored; check Query
            // manually."), and there's no other field on this table to
            // anchor an INSERT that omits ID, so there's no known SQLRunner
            // syntax to insert a row into an AUTOINC-only table. See
            // AUTOALPIDX below for the workaround used when a second field
            // exists (omit ID entirely and let it auto-assign).

            // 4. INTEGER primary key + an ALPHA field (no secondary index on it).
            cases.Add(new CaseDefinition
            {
                Name = "PKALPHA",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
                        new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (ID INTEGER, NAME CHARACTER(20), PRIMARY KEY (ID))"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (ID, NAME) VALUES (1, 'abc')",
                InsertOurs = new object[] { 1, "abc" },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (ID, NAME) VALUES (" + (i + 1) + ", 'abc" + i + "')"
            });

            // 5. INTEGER primary key + ALPHA field which is ALSO a secondary index.
            cases.Add(new CaseDefinition
            {
                Name = "PKALPHIDX",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
                        new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                    },
                    Indexes =
                    {
                        new TableIndexDefinition(new[] { 1 })
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (ID INTEGER, NAME CHARACTER(20), PRIMARY KEY (ID))",
                    "CREATE INDEX NAMEIDX ON '{PATH}' (NAME)"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (ID, NAME) VALUES (1, 'abc')",
                InsertOurs = new object[] { 1, "abc" },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (ID, NAME) VALUES (" + (i + 1) + ", 'abc" + i + "')"
            });

            // 6. AUTOINC primary key + ALPHA field which is ALSO a secondary index.
            cases.Add(new CaseDefinition
            {
                Name = "AUTOALPIDX",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("ID", ParadoxFieldTypes.AutoInc, 4, true),
                        new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                    },
                    Indexes =
                    {
                        new TableIndexDefinition(new[] { 1 })
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (ID AUTOINC, NAME CHARACTER(20), PRIMARY KEY (ID))",
                    "CREATE INDEX NAMEIDX ON '{PATH}' (NAME)"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (NAME) VALUES ('abc')",
                InsertOurs = new object[] { null, "abc" },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (NAME) VALUES ('abc" + i + "')"
            });

            // 7. INTEGER primary key + ALPHA field + a MEMO field (left untouched
            // - only ID/NAME are ever written, to keep this minimal).
            cases.Add(new CaseDefinition
            {
                Name = "PKALPMEMO",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
                        new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                        // CreateMemoField takes the leader (inline byte) size directly,
                        // matching the "20" in SQL's BLOB(20,1) DDL below - the on-disk
                        // fSize=30 (leader+10 pointer bytes) is computed automatically.
                        TableFieldDefinition.CreateMemoField("NOTES", ParadoxFieldTypes.MemoBLOb, 20),
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (ID INTEGER, NAME CHARACTER(20), NOTES BLOB(20,1), PRIMARY KEY (ID))"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (ID, NAME) VALUES (1, 'abc')",
                InsertOurs = new object[] { 1, "abc", null },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (ID, NAME) VALUES (" + (i + 1) + ", 'abc" + i + "')"
            });

            // 8. INTEGER primary key + ALPHA field + a BLOb field (left untouched).
            cases.Add(new CaseDefinition
            {
                Name = "PKALPBLOB",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
                        new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                        TableFieldDefinition.CreateMemoField("FILEVAL", ParadoxFieldTypes.BLOb, 240),
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (ID INTEGER, NAME CHARACTER(20), FILEVAL BLOB(240,2), PRIMARY KEY (ID))"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (ID, NAME) VALUES (1, 'abc')",
                InsertOurs = new object[] { 1, "abc", null },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (ID, NAME) VALUES (" + (i + 1) + ", 'abc" + i + "')"
            });

            // 9. AUTOINC primary key + ALPHA field + a MEMO field (left untouched).
            cases.Add(new CaseDefinition
            {
                Name = "AUTOALPMEMO",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("ID", ParadoxFieldTypes.AutoInc, 4, true),
                        new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                        TableFieldDefinition.CreateMemoField("NOTES", ParadoxFieldTypes.MemoBLOb, 20),
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (ID AUTOINC, NAME CHARACTER(20), NOTES BLOB(20,1), PRIMARY KEY (ID))"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (NAME) VALUES ('abc')",
                InsertOurs = new object[] { null, "abc", null },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (NAME) VALUES ('abc" + i + "')"
            });

            // 10. AUTOINC primary key + ALPHA field + a BLOb field (left untouched).
            cases.Add(new CaseDefinition
            {
                Name = "AUTOALPBLOB",
                Schema = new TableSchemaDefinition
                {
                    Fields =
                    {
                        new TableFieldDefinition("ID", ParadoxFieldTypes.AutoInc, 4, true),
                        new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                        TableFieldDefinition.CreateMemoField("FILEVAL", ParadoxFieldTypes.BLOb, 240),
                    }
                },
                SqlRunnerDdl = new List<string>
                {
                    "CREATE TABLE '{PATH}' (ID AUTOINC, NAME CHARACTER(20), FILEVAL BLOB(240,2), PRIMARY KEY (ID))"
                },
                InsertSqlRunner = "INSERT INTO '{PATH}' (NAME) VALUES ('abc')",
                InsertOurs = new object[] { null, "abc", null },
                InsertSqlRunnerForRow = i => "INSERT INTO '{PATH}' (NAME) VALUES ('abc" + i + "')"
            });

            return cases;
        }

        internal static TableSchemaDefinition CloneSchemaWithName(TableSchemaDefinition schema, string baseName)
        {
            var clone = new TableSchemaDefinition { TableName = baseName };
            foreach (var f in schema.Fields)
                clone.Fields.Add(f.Clone());
            foreach (var idx in schema.Indexes)
                clone.Indexes.Add(new TableIndexDefinition(idx.FieldIndices));
            return clone;
        }

        // --------------------------------------------------------------
        // Header comparison
        // --------------------------------------------------------------

        /// <summary>
        /// Finds every file sharing <paramref name="baseName"/> in each
        /// directory, pairs them up by extension, and byte-diffs each pair's
        /// header region (the file's own declared HeaderSize, or the whole
        /// file if shorter). Returns the total number of differing bytes
        /// found (0 = identical headers across every paired file).
        /// </summary>
        private static int CompareAllHeaders(string oursDir, string sqlDir, string baseName)
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
                if (ext.Equals(".MB", StringComparison.OrdinalIgnoreCase))
                    continue; // blob files have no comparable "header" in the same sense

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

                totalDiffs += CompareHeaderPair(ext, oursFiles[ext], sqlFiles[ext]);
            }

            if (totalDiffs == 0)
                Console.WriteLine("  All paired headers byte-identical.");

            return totalDiffs;
        }

        private static int CompareHeaderPair(string ext, string oursPath, string sqlPath)
        {
            byte[] oursHeader = ReadHeaderRegion(oursPath);
            byte[] sqlHeader = ReadHeaderRegion(sqlPath);

            int minLen = Math.Min(oursHeader.Length, sqlHeader.Length);
            int maxLen = Math.Max(oursHeader.Length, sqlHeader.Length);
            int diffCount = 0;

            if (oursHeader.Length != sqlHeader.Length)
            {
                Console.WriteLine("  [{0}] HeaderSize differs: ours={1} sqlrunner={2}", ext, oursHeader.Length, sqlHeader.Length);
                diffCount++;
            }

            for (int i = 0; i < minLen; i++)
            {
                if (oursHeader[i] != sqlHeader[i])
                {
                    string fieldName = DescribeOffset(i);
                    Console.WriteLine("  [{0}] offset 0x{1:X2}{2}: ours=0x{3:X2} sqlrunner=0x{4:X2}",
                        ext, i, fieldName != null ? " (" + fieldName + ")" : "", oursHeader[i], sqlHeader[i]);
                    diffCount++;
                }
            }

            if (maxLen > minLen)
            {
                Console.WriteLine("  [{0}] {1} extra trailing byte(s) beyond the shorter header not compared.", ext, maxLen - minLen);
            }

            return diffCount;
        }

        /// <summary>
        /// Reads a file's header region, sized by its own declared HeaderSize
        /// field (offset 0x02, uint16), clamped to the file's actual length.
        /// </summary>
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

        /// <summary>
        /// Best-effort annotation of a header offset against the known named
        /// fields in <see cref="ParadoxHeaderOffsets"/>, for readability in
        /// diff output. Not exhaustive - only the fields this library
        /// actively reads/writes are named; everything else just shows as a
        /// raw offset.
        /// </summary>
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
        // SQLRunner process invocation (mirrors CorpusTest.RunSqlRunner)
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

        private static void RunSqlRunner(string sql) => SqlRunner.Execute(sql);
    }
}
