using System;
using System.IO;
using ParadoxReader;

namespace ParadoxTest
{
    internal static class LibUpdateTest
    {
        public static void RunCreateTest()
        {
            var dbDir = @"C:\TEMP\libtest";
            var tableName = "TESTTAB_CREATE.DB";

            using (var table = new ParadoxTableFile(dbDir, tableName))
            {
                var fieldValues = new object[table.FieldCount];
                // ID is AutoInc (assigned automatically), SECONDARYID = 1
                fieldValues[1] = 1;

                var rec = table.AppendRecord(fieldValues);
                Console.WriteLine("Appended record via library AppendRecord(), block={0} idx={1}", rec.BlockNumber, rec.RecordIndex);
            }
        }

        public static void Run()
        {
            var dbDir = @"C:\TEMP";
            var tableName = "testtabmemo_77lib.DB";

            using (var table = new ParadoxTableFile(dbDir, tableName))
            {
                foreach (var rec in table.Enumerate())
                {
                    var idValue = rec.DataValues[0];
                    if (Convert.ToInt32(idValue) != 1)
                        continue;

                    var newValues = new object[rec.DataValues.Length];
                    Array.Copy(rec.DataValues, newValues, rec.DataValues.Length);

                    newValues[8] = new TimeSpan(22, 22, 22);

                    var origMemo = newValues[13] as MemoValue;
                    newValues[13] = new MemoValue("sqlrunnertest", origMemo?.BlobInfo);

                    table.UpdateRecord(rec, newValues);
                    Console.WriteLine("Updated record id=1 via library UpdateRecord()");
                    return;
                }
            }

            Console.WriteLine("Record id=1 not found");
        }
    }
}
