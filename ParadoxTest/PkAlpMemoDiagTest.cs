using System;
using System.IO;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Diagnostic harness for the PKALPMEMO memo-value-set path specifically.
    /// Mirrors the exact SQLRunner flow used to recreate the reference
    /// fixture (CREATE TABLE, INSERT ID/NAME, then UPDATE ... SET NOTES =
    /// '...' WHERE ID = 1), but using ParadoxReader's own TableCreator +
    /// AppendRecord + UpdateRecord, so the two on-disk results can be
    /// byte-diffed to find where our memo-write path diverges from BDE.
    /// </summary>
    internal static class PkAlpMemoDiagTest
    {
        public static void Run()
        {
            string dir = @"C:\TEMP\createtablecompare\PKALPMEMO\ours";
            string dbPath = Path.Combine(dir, "PKALPMEM.DB");

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

            var schema = new TableSchemaDefinition
            {
                TableName = "PKALPMEM",
                Fields =
                {
                    new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
                    new TableFieldDefinition("NAME", ParadoxFieldTypes.Alpha, 20, false),
                    new TableFieldDefinition("NOTES", ParadoxFieldTypes.MemoBLOb, 250, false),
                }
            };

            TableCreator.CreateNew(dbPath, schema);
            Console.WriteLine("Created {0}", dbPath);

            using (var table = new ParadoxTableFile(dbPath))
            {
                var rec = table.AppendRecord(new object[] { 1, "abc", null });
                Console.WriteLine("Appended record ID=1, NAME=abc, block={0} idx={1}", rec.BlockNumber, rec.RecordIndex);
            }

            using (var table = new ParadoxTableFile(dbPath))
            {
                foreach (var rec in table.Enumerate())
                {
                    var idValue = rec.DataValues[0];
                    if (Convert.ToInt32(idValue) != 1)
                        continue;

                    var newValues = new object[rec.DataValues.Length];
                    Array.Copy(rec.DataValues, newValues, rec.DataValues.Length);

                    var origMemo = newValues[2] as MemoValue;
                    newValues[2] = new MemoValue("test sql memo set", origMemo?.BlobInfo);

                    table.UpdateRecord(rec, newValues);
                    Console.WriteLine("Updated record id=1 with memo value via library UpdateRecord()");
                    break;
                }
            }

            using (var table = new ParadoxTableFile(dbPath))
            {
                foreach (var rec in table.Enumerate())
                {
                    var memo = rec.DataValues[2] as MemoValue;
                    Console.WriteLine("Readback: ID={0} NAME={1} NOTES={2}", rec.DataValues[0], rec.DataValues[1], memo?.Text ?? "(null)");
                }
            }
        }
    }
}
