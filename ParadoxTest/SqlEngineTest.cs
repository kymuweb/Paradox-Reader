using System;
using System.Data;
using System.IO;
using System.Linq;
using ParadoxReader;
using ParadoxReader.Sql;

namespace ParadoxTest
{
    /// <summary>
    /// Diagnostic/regression mode for <see cref="ParadoxSqlExecutor"/> and the
    /// ADO.NET wrapper (<see cref="ParadoxConnection"/>/<see cref="ParadoxCommand"/>).
    /// Exercises SELECT (full-scan and index-accelerated), INSERT, UPDATE, and
    /// DELETE against a copy of TESTTAB.DB, cross-checking results against the
    /// existing direct ParadoxTableFile API to make sure the SQL layer agrees
    /// with the library it's built on.
    /// </summary>
    internal static class SqlEngineTest
    {
        private const string TestFolder = @"c:\temp\sqlenginetest";
        private static string TestTabTableName = "TESTTAB.DB";

        private static string SourceDataFolder =>
            Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location), "data");

        public static void Run()
        {
            ResetTestFolder();
            SeedRecords();

            bool allOk = true;
            allOk &= TestSelectStarFullScan();
            allOk &= TestSelectWithWhereFullScan();
            allOk &= TestSelectWithPrimaryIndexEquality();
            allOk &= TestSelectColumnProjection();
            allOk &= TestInsert();
            allOk &= TestUpdate();
            allOk &= TestDelete();
            allOk &= TestAdoNetConnectionWrapper();
            allOk &= TestParameterizedSelectAndInsert();
            allOk &= TestGetSchemaTable();

            Console.WriteLine();
            Console.WriteLine(allOk ? "SqlEngine test PASSED." : "SqlEngine test FAILED.");
        }

        private static void ResetTestFolder()
        {
            Directory.CreateDirectory(TestFolder);

            var tableBaseName = Path.GetFileNameWithoutExtension(TestTabTableName);
            foreach (var existingFile in Directory.GetFiles(TestFolder, tableBaseName + ".*"))
            {
                try { File.Delete(existingFile); } catch { /* best effort */ }
            }

            foreach (var sourceFile in Directory.GetFiles(SourceDataFolder, tableBaseName + ".*"))
            {
                var destFile = Path.Combine(TestFolder, Path.GetFileName(sourceFile));
                File.Copy(sourceFile, destFile, overwrite: true);
            }

            Console.WriteLine("Test folder reset: {0}", TestFolder);
        }

        private static string DbPath => Path.Combine(TestFolder, TestTabTableName);

        /// <summary>
        /// Seeds 5 records into the empty TESTTAB.DB fixture, matching the
        /// field layout/values used by MiscTests.MakeSampleFieldValues, so
        /// the SQL tests below (which reference SECVAL/ID/CHARVAL/etc.) have
        /// data to work against.
        /// </summary>
        private static void SeedRecords()
        {
            using (var table = new ParadoxTableFile(DbPath))
            {
                for (short i = 1; i <= 5; i++)
                {
                    var values = new object[15];
                    values[0]  = null; // ID (AUTOINC)
                    values[1]  = i;    // SECVAL
                    values[2]  = 100 + i; // INTVAL
                    values[3]  = Math.Round(10m + i, 2); // DECVAL
                    values[4]  = Math.Round(20m + i, 2); // NUMVAL
                    values[5]  = Math.Round(1.5d + i, 2); // FLOATVAL
                    values[6]  = "CHAR-append" + i; // CHARVAL
                    values[7]  = "VARCHAR-append" + i; // VARCHARVAL
                    values[8]  = new DateTime(2026, 1, 1).AddDays(i); // DATEVAL
                    values[9]  = i % 2 == 0; // BOOLVAL
                    values[10] = new MemoValue("memo text for append" + i, null); // MEMOVAL
                    values[11] = new TimeSpan(1, 2, 3); // TIMEVAL
                    values[12] = new DateTime(2026, 1, 1, 12, 0, 0).AddMinutes(i); // TIMESTAMPVAL
                    values[13] = Math.Round(100m + i, 2); // MONEYVAL
                    values[14] = new byte[] { (byte)i, 1, 2, 3, 4, 5, 6, 7, 8, 9 }; // BYTESVAL
                    table.AppendRecord(values);
                }
            }
            Console.WriteLine("Seeded 5 record(s) into {0}", DbPath);
        }

        // --------------------------------------------------------------------

        private static bool TestSelectStarFullScan()
        {
            Console.WriteLine("=== Test: SELECT * (full scan) ===");
            try
            {
                int directCount;
                using (var table = new ParadoxTableFile(DbPath))
                    directCount = table.Enumerate().Count();

                using (var executor = new ParadoxSqlExecutor())
                using (var reader = executor.ExecuteReader($"SELECT * FROM '{DbPath}'"))
                {
                    int sqlCount = 0;
                    while (reader.Read()) sqlCount++;

                    if (sqlCount != directCount)
                    {
                        Console.WriteLine("  [FAIL] SELECT * returned {0} row(s), expected {1}", sqlCount, directCount);
                        return false;
                    }
                    Console.WriteLine("  [ok] SELECT * returned {0} row(s), matching direct Enumerate().", sqlCount);
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestSelectWithWhereFullScan()
        {
            Console.WriteLine("=== Test: SELECT * WHERE (non-indexed field, full scan) ===");
            try
            {
                int directCount;
                using (var table = new ParadoxTableFile(DbPath))
                    directCount = table.Enumerate(r => Convert.ToBoolean(r.DataValues[9])).Count(); // BOOLVAL is field 9

                using (var executor = new ParadoxSqlExecutor())
                using (var reader = executor.ExecuteReader($"SELECT * FROM '{DbPath}' WHERE BOOLVAL = TRUE"))
                {
                    int sqlCount = 0;
                    while (reader.Read()) sqlCount++;

                    if (sqlCount != directCount)
                    {
                        Console.WriteLine("  [FAIL] SELECT WHERE BOOLVAL=TRUE returned {0} row(s), expected {1}", sqlCount, directCount);
                        return false;
                    }
                    Console.WriteLine("  [ok] SELECT WHERE BOOLVAL=TRUE returned {0} row(s), matching direct Enumerate(predicate).", sqlCount);
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestSelectWithPrimaryIndexEquality()
        {
            Console.WriteLine("=== Test: SELECT * WHERE ID = <n> (primary index) ===");
            try
            {
                using (var executor = new ParadoxSqlExecutor())
                using (var reader = executor.ExecuteReader($"SELECT * FROM '{DbPath}' WHERE ID = 1"))
                {
                    if (!reader.Read())
                    {
                        Console.WriteLine("  [FAIL] SELECT WHERE ID=1 returned no rows.");
                        return false;
                    }
                    int idVal = reader.GetInt32(reader.GetOrdinal("ID"));
                    if (idVal != 1)
                    {
                        Console.WriteLine("  [FAIL] SELECT WHERE ID=1 returned ID={0}, expected 1", idVal);
                        return false;
                    }
                    if (reader.Read())
                    {
                        Console.WriteLine("  [FAIL] SELECT WHERE ID=1 returned more than one row.");
                        return false;
                    }
                    Console.WriteLine("  [ok] SELECT WHERE ID=1 returned exactly one row with ID=1.");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestSelectColumnProjection()
        {
            Console.WriteLine("=== Test: SELECT <columns> (projection) ===");
            try
            {
                using (var executor = new ParadoxSqlExecutor())
                using (var reader = executor.ExecuteReader($"SELECT ID, SECVAL FROM '{DbPath}' WHERE ID = 1"))
                {
                    if (reader.FieldCount != 2)
                    {
                        Console.WriteLine("  [FAIL] Expected FieldCount=2, got {0}", reader.FieldCount);
                        return false;
                    }
                    if (!reader.Read())
                    {
                        Console.WriteLine("  [FAIL] No rows returned.");
                        return false;
                    }
                    if (!string.Equals(reader.GetName(0), "ID", StringComparison.OrdinalIgnoreCase) ||
                        !string.Equals(reader.GetName(1), "SECVAL", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine("  [FAIL] Column names mismatch: {0}, {1}", reader.GetName(0), reader.GetName(1));
                        return false;
                    }
                    Console.WriteLine("  [ok] Projected columns ID={0}, SECVAL={1}", reader.GetValue(0), reader.GetValue(1));
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestInsert()
        {
            Console.WriteLine("=== Test: INSERT ===");
            try
            {
                int beforeCount;
                using (var table = new ParadoxTableFile(DbPath))
                    beforeCount = table.Enumerate().Count();

                using (var executor = new ParadoxSqlExecutor())
                {
                    int affected = executor.ExecuteNonQuery(
                        $"INSERT INTO '{DbPath}' (SECVAL, INTVAL, CHARVAL, VARCHARVAL, DATEVAL, BOOLVAL, TIMEVAL, TIMESTAMPVAL, MONEYVAL, DECVAL, NUMVAL, FLOATVAL) " +
                        "VALUES (99, 199, 'CHAR-sqltest', 'VARCHAR-sqltest', '01/15/2026', TRUE, '01:02:03', '01/15/2026 12:00:00', 123.45, 10.5, 20.5, 1.5)");

                    if (affected != 1)
                    {
                        Console.WriteLine("  [FAIL] INSERT reported {0} affected row(s), expected 1", affected);
                        return false;
                    }
                }

                int afterCount;
                using (var table = new ParadoxTableFile(DbPath))
                {
                    afterCount = table.Enumerate().Count();
                    var inserted = table.Enumerate(r => Convert.ToInt32(r.DataValues[1]) == 99).FirstOrDefault();
                    if (inserted == null)
                    {
                        Console.WriteLine("  [FAIL] Inserted row (SECVAL=99) not found via direct Enumerate.");
                        return false;
                    }
                    if ((string)inserted.DataValues[6] != "CHAR-sqltest")
                    {
                        Console.WriteLine("  [FAIL] Inserted CHARVAL mismatch: '{0}'", inserted.DataValues[6]);
                        return false;
                    }
                }

                if (afterCount != beforeCount + 1)
                {
                    Console.WriteLine("  [FAIL] Row count after INSERT = {0}, expected {1}", afterCount, beforeCount + 1);
                    return false;
                }

                Console.WriteLine("  [ok] INSERT added 1 row (count {0} -> {1}), verified via direct Enumerate.", beforeCount, afterCount);
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestUpdate()
        {
            Console.WriteLine("=== Test: UPDATE ===");
            try
            {
                using (var executor = new ParadoxSqlExecutor())
                {
                    int affected = executor.ExecuteNonQuery(
                        $"UPDATE '{DbPath}' SET CHARVAL = 'CHAR-updated', INTVAL = 999999 WHERE SECVAL = 3");

                    if (affected != 1)
                    {
                        Console.WriteLine("  [FAIL] UPDATE reported {0} affected row(s), expected 1", affected);
                        return false;
                    }
                }

                using (var table = new ParadoxTableFile(DbPath))
                {
                    var updated = table.Enumerate(r => Convert.ToInt32(r.DataValues[1]) == 3).FirstOrDefault();
                    if (updated == null)
                    {
                        Console.WriteLine("  [FAIL] Row with SECVAL=3 not found after UPDATE.");
                        return false;
                    }
                    if ((string)updated.DataValues[6] != "CHAR-updated" || Convert.ToInt32(updated.DataValues[2]) != 999999)
                    {
                        Console.WriteLine("  [FAIL] UPDATE didn't apply: CHARVAL='{0}', INTVAL={1}",
                            updated.DataValues[6], updated.DataValues[2]);
                        return false;
                    }
                }

                Console.WriteLine("  [ok] UPDATE applied CHARVAL/INTVAL changes, verified via direct Enumerate.");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestDelete()
        {
            Console.WriteLine("=== Test: DELETE ===");
            try
            {
                int beforeCount;
                using (var table = new ParadoxTableFile(DbPath))
                    beforeCount = table.Enumerate().Count();

                using (var executor = new ParadoxSqlExecutor())
                {
                    int affected = executor.ExecuteNonQuery($"DELETE FROM '{DbPath}' WHERE SECVAL = 99");
                    if (affected != 1)
                    {
                        Console.WriteLine("  [FAIL] DELETE reported {0} affected row(s), expected 1", affected);
                        return false;
                    }
                }

                using (var table = new ParadoxTableFile(DbPath))
                {
                    int afterCount = table.Enumerate().Count();
                    if (afterCount != beforeCount - 1)
                    {
                        Console.WriteLine("  [FAIL] Row count after DELETE = {0}, expected {1}", afterCount, beforeCount - 1);
                        return false;
                    }
                    if (table.Enumerate(r => Convert.ToInt32(r.DataValues[1]) == 99).Any())
                    {
                        Console.WriteLine("  [FAIL] Row with SECVAL=99 still present after DELETE.");
                        return false;
                    }
                }

                Console.WriteLine("  [ok] DELETE removed the inserted row (SECVAL=99), verified via direct Enumerate.");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestAdoNetConnectionWrapper()
        {
            Console.WriteLine("=== Test: ParadoxConnection/ParadoxCommand (ADO.NET wrapper) ===");
            try
            {
                using (IDbConnection conn = new ParadoxConnection(TestFolder))
                {
                    conn.Open();
                    using (var cmd = conn.CreateCommand())
                    {
                        // Exercise bare table-name resolution via ConnectionString base dir.
                        cmd.CommandText = $"SELECT ID, SECVAL FROM '{TestTabTableName}' WHERE SECVAL = 1";
                        using (var reader = cmd.ExecuteReader())
                        {
                            if (!reader.Read())
                            {
                                Console.WriteLine("  [FAIL] ADO.NET wrapper SELECT returned no rows.");
                                return false;
                            }
                            Console.WriteLine("  [ok] ADO.NET wrapper SELECT (bare table name via ConnectionString base dir) returned ID={0}, SECVAL={1}",
                                reader.GetValue(0), reader.GetValue(1));
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestParameterizedSelectAndInsert()
        {
            Console.WriteLine("=== Test: Parameterized SELECT/INSERT (@name and ? placeholders) ===");
            try
            {
                using (IDbConnection conn = new ParadoxConnection(TestFolder))
                {
                    conn.Open();

                    // Named parameter in WHERE.
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $"SELECT ID, SECVAL FROM '{TestTabTableName}' WHERE SECVAL = @secval";
                        cmd.Parameters.Add(new ParadoxParameter("@secval", 2));
                        using (var reader = cmd.ExecuteReader())
                        {
                            if (!reader.Read() || Convert.ToInt32(reader.GetValue(1)) != 2)
                            {
                                Console.WriteLine("  [FAIL] Named parameter SELECT did not return SECVAL=2.");
                                return false;
                            }
                            Console.WriteLine("  [ok] Named parameter (@secval) SELECT returned ID={0}, SECVAL={1}",
                                reader.GetValue(0), reader.GetValue(1));
                        }
                    }

                    // Positional '?' parameter in WHERE.
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = $"SELECT ID, SECVAL FROM '{TestTabTableName}' WHERE SECVAL = ?";
                        cmd.Parameters.Add(new ParadoxParameter { Value = 4 });
                        using (var reader = cmd.ExecuteReader())
                        {
                            if (!reader.Read() || Convert.ToInt32(reader.GetValue(1)) != 4)
                            {
                                Console.WriteLine("  [FAIL] Positional parameter SELECT did not return SECVAL=4.");
                                return false;
                            }
                            Console.WriteLine("  [ok] Positional parameter (?) SELECT returned ID={0}, SECVAL={1}",
                                reader.GetValue(0), reader.GetValue(1));
                        }
                    }

                    // Parameterized INSERT.
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText =
                            $"INSERT INTO '{TestTabTableName}' (SECVAL, CHARVAL) VALUES (@secval, @charval)";
                        cmd.Parameters.Add(new ParadoxParameter("@secval", 555));
                        cmd.Parameters.Add(new ParadoxParameter("@charval", "CHAR-param"));
                        int affected = cmd.ExecuteNonQuery();
                        if (affected != 1)
                        {
                            Console.WriteLine("  [FAIL] Parameterized INSERT reported {0} affected row(s), expected 1.", affected);
                            return false;
                        }
                    }
                }

                using (var table = new ParadoxTableFile(DbPath))
                {
                    var inserted = table.Enumerate(r => Convert.ToInt32(r.DataValues[1]) == 555).FirstOrDefault();
                    if (inserted == null || (string)inserted.DataValues[6] != "CHAR-param")
                    {
                        Console.WriteLine("  [FAIL] Parameterized INSERT row not found or CHARVAL mismatch.");
                        return false;
                    }
                    table.DeleteRecord(inserted); // clean up so later assertions aren't affected
                }

                Console.WriteLine("  [ok] Parameterized INSERT applied SECVAL=555, CHARVAL='CHAR-param'.");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }

        private static bool TestGetSchemaTable()
        {
            Console.WriteLine("=== Test: SqlDataReader.GetSchemaTable() ===");
            try
            {
                using (var executor = new ParadoxSqlExecutor())
                using (var reader = executor.ExecuteReader($"SELECT ID, SECVAL, CHARVAL FROM '{DbPath}'"))
                {
                    var schema = reader.GetSchemaTable();
                    if (schema == null || schema.Rows.Count != 3)
                    {
                        Console.WriteLine("  [FAIL] GetSchemaTable() returned {0} row(s), expected 3.",
                            schema?.Rows.Count.ToString() ?? "null");
                        return false;
                    }

                    var idRow = schema.Rows[0];
                    if (!string.Equals((string)idRow["ColumnName"], "ID", StringComparison.OrdinalIgnoreCase) ||
                        (Type)idRow["DataType"] != typeof(int) || !(bool)idRow["IsAutoIncrement"])
                    {
                        Console.WriteLine("  [FAIL] Unexpected schema for ID column: Name={0}, Type={1}, AutoInc={2}",
                            idRow["ColumnName"], idRow["DataType"], idRow["IsAutoIncrement"]);
                        return false;
                    }

                    Console.WriteLine("  [ok] GetSchemaTable() returned {0} column(s); ID is AutoInc={1}, DataType={2}.",
                        schema.Rows.Count, idRow["IsAutoIncrement"], idRow["DataType"]);
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("  [FAIL] Exception: {0}", ex);
                return false;
            }
        }
    }
}
