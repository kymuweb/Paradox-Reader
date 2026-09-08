using System;
using System.IO;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Diagnostic harness for the PKALPMEMO memo-value-set path specifically.
    /// Mirrors the exact SQLRunner flow used to recreate the reference
    /// fixture (CREATE TABLE, INSERT ID/NAME per row, then UPDATE ... SET
    /// NOTES = '...' WHERE ID = n), but using ParadoxReader's own
    /// TableCreator + AppendRecord + UpdateRecord, so the two on-disk
    /// results can be byte-diffed to find where our memo-write path
    /// diverges from BDE.
    ///
    /// The NOTES field is fSize=30 (leader = fSize-10 = 20 inline bytes),
    /// so this specifically exercises the inline/leader boundary with memo
    /// text of length 19 (fits inline, one byte to spare), 20 (fits exactly),
    /// and 21 (one byte too many, must externalize to the .MB file) for
    /// rows ID=1, 2, 3 respectively.
    /// </summary>
    internal static class PkAlpMemoDiagTest
    {
        private static readonly string[] MemoValues =
        {
            new string('a', 19), // fits inline (leader=20, 1 byte to spare)
            new string('b', 20), // fits inline exactly (leader=20)
            new string('c', 21), // does not fit inline, must externalize to .MB
        };

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
                    TableFieldDefinition.CreateBlobField("NOTES", ParadoxFieldTypes.MemoBLOb, 20),
                }
            };

            TableCreator.CreateNew(dbPath, schema);
            Console.WriteLine("Created {0}", dbPath);

            using (var table = new ParadoxTableFile(dbPath))
            {
                for (int i = 0; i < MemoValues.Length; i++)
                {
                    int id = i + 1;
                    var rec = table.AppendRecord(new object[] { id, "abc" + i, null });
                    Console.WriteLine("Appended record ID={0}, NAME=abc{1}, block={2} idx={3}", id, i, rec.BlockNumber, rec.RecordIndex);
                }
            }

            using (var table = new ParadoxTableFile(dbPath))
            {
                foreach (var rec in table.Enumerate())
                {
                    var idValue = Convert.ToInt32(rec.DataValues[0]);
                    if (idValue < 1 || idValue > MemoValues.Length)
                        continue;

                    var newValues = new object[rec.DataValues.Length];
                    Array.Copy(rec.DataValues, newValues, rec.DataValues.Length);

                    var origMemo = newValues[2] as MemoValue;
                    string memoText = MemoValues[idValue - 1];
                    newValues[2] = new MemoValue(memoText, origMemo?.BlobInfo);

                    table.UpdateRecord(rec, newValues);
                    Console.WriteLine("Updated record id={0} with memo value (len={1}) via library UpdateRecord()", idValue, memoText.Length);
                }
            }

            using (var table = new ParadoxTableFile(dbPath))
            {
                foreach (var rec in table.Enumerate())
                {
                    var memo = rec.DataValues[2] as MemoValue;
                    Console.WriteLine("Readback: ID={0} NAME={1} NOTES(len={2})={3}", rec.DataValues[0], rec.DataValues[1], memo?.Text?.Length ?? -1, memo?.Text ?? "(null)");
                }
            }
        }
    }
}
