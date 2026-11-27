using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ParadoxReader;

namespace ParadoxTest
{
    /// <summary>
    /// Verifies duplicate-key rejection (primary key, unique case-sensitive / case-insensitive,
    /// composite, and non-unique indexes) and that a rejected write leaves every file byte-identical.
    /// Run with: ParadoxTest dupkeytest
    /// </summary>
    internal static class DuplicateKeyTest
    {
        private static int failures;

        public static int Run()
        {
            string dir = Path.Combine(Path.GetTempPath(), "dupkeytest_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            string db = Path.Combine(dir, "DUPKEY.DB");

            var schema = new TableSchemaDefinition
            {
                Fields =
                {
                    new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
                    new TableFieldDefinition("UCI", ParadoxFieldTypes.Alpha, 20, false),
                    new TableFieldDefinition("UCS", ParadoxFieldTypes.Alpha, 20, false),
                    new TableFieldDefinition("A", ParadoxFieldTypes.Alpha, 10, false),
                    new TableFieldDefinition("B", ParadoxFieldTypes.Long, 4, false),
                    new TableFieldDefinition("NU", ParadoxFieldTypes.Alpha, 20, false),
                },
                Indexes =
                {
                    new TableIndexDefinition(new[] { 1 }) { IsUnique = true, IsCaseSensitive = false },
                    new TableIndexDefinition(new[] { 2 }) { IsUnique = true, IsCaseSensitive = true },
                    new TableIndexDefinition(new[] { 3, 4 }) { IsUnique = true, IsCaseSensitive = true },
                    new TableIndexDefinition(new[] { 5 }),
                }
            };
            TableCreator.CreateNew(db, schema);

            using (var t = new ParadoxTableFile(db))
            {
                var flags = t.SecondaryIndexes.Select(i => (i.IsUnique, i.IsCaseSensitive)).ToList();
                Check("flags round-trip", flags.Contains((true, false)) && flags.Contains((true, true)) && flags.Contains((false, true)));
                t.AppendRecord(new object[] { 1, "Abc", "Abc", "x", 1, "n" });
            }

            Expect(db, "pk duplicate", new object[] { 1, "Q1", "Q1", "q", 9, "n" }, true);
            Expect(db, "unique case-insens: different case", new object[] { 2, "ABC", "Q2", "q", 9, "n" }, true);
            Expect(db, "unique case-sens: exact duplicate", new object[] { 3, "Q3", "Abc", "q", 9, "n" }, true);
            Expect(db, "unique case-sens: different case allowed", new object[] { 4, "Q4", "abc", "q", 9, "n" }, false);
            Expect(db, "composite: all fields equal", new object[] { 5, "Q5", "Q5", "x", 1, "n" }, true);
            Expect(db, "composite: one field differs", new object[] { 6, "Q6", "Q6", "x", 2, "n" }, false);
            Expect(db, "non-unique duplicate allowed", new object[] { 7, "Q7", "Q7", "y", 3, "n" }, false);

            try { Directory.Delete(dir, true); } catch { }
            ManyRows();
            Console.WriteLine(failures == 0 ? "[dupkeytest] ALL PASSED" : $"[dupkeytest] {failures} FAILED");
            return failures;
        }

        private static void Expect(string db, string name, object[] row, bool shouldThrow)
        {
            var before = Snapshot(db);
            bool threw = false;
            try
            {
                using (var t = new ParadoxTableFile(db)) t.AppendRecord(row);
            }
            catch (KeyViolationException) { threw = true; }
            catch (Exception ex) { Console.WriteLine($"  unexpected {ex.GetType().Name}: {ex.Message}"); }

            bool ok = threw == shouldThrow;
            if (shouldThrow && ok)
                ok = Snapshot(db).SequenceEqual(before, new SnapshotComparer());
            Check(name, ok);
        }

		private static void ManyRows()
		{
			string dir = Path.Combine(Path.GetTempPath(), "dupkeymany_" + Guid.NewGuid().ToString("N"));
			Directory.CreateDirectory(dir);
			string db = Path.Combine(dir, "MANY.DB");
			var schema = new TableSchemaDefinition
			{
				Fields =
				{
					new TableFieldDefinition("ID", ParadoxFieldTypes.Long, 4, true),
					new TableFieldDefinition("UCI", ParadoxFieldTypes.Alpha, 20, false),
					new TableFieldDefinition("UCS", ParadoxFieldTypes.Alpha, 20, false),
				},
				Indexes =
				{
					new TableIndexDefinition(new[] { 1 }) { IsUnique = true, IsCaseSensitive = false },
					new TableIndexDefinition(new[] { 2 }) { IsUnique = true, IsCaseSensitive = true },
				}
			};
			TableCreator.CreateNew(db, schema);
			var rng = new Random(5);
			var names = new List<string>();
			using (var t = new ParadoxTableFile(db))
			{
				for (int i = 1; i <= 600; i++)
				{
					string n = "Name" + rng.Next(0, 1000000).ToString("D7") + (char)('a' + rng.Next(26));
					if (names.Contains(n, StringComparer.OrdinalIgnoreCase)) continue;
					names.Add(n);
					t.AppendRecord(new object[] { i, n, n });
				}
			}
			int missedCi = 0, missedCs = 0, falsePos = 0, next = 100000;
			using (var t = new ParadoxTableFile(db))
			{
				foreach (var n in names.OrderBy(_ => rng.Next()).Take(150))
				{
					try { t.AppendRecord(new object[] { next++, n.ToUpperInvariant(), "zz" + next }); missedCi++; } catch (KeyViolationException) { }
					try { t.AppendRecord(new object[] { next++, "zz" + next, n }); missedCs++; } catch (KeyViolationException) { }
					try { t.AppendRecord(new object[] { next++, "fresh" + next, "fresh" + next }); } catch (KeyViolationException) { falsePos++; }
				}
			}
			Check($"many rows: CI duplicates missed={missedCi}", missedCi == 0);
			Check($"many rows: CS duplicates missed={missedCs}", missedCs == 0);
			Check($"many rows: false positives={falsePos}", falsePos == 0);
			try { Directory.Delete(dir, true); } catch { }
		}
        private static List<KeyValuePair<string, byte[]>> Snapshot(string db)
        {
            string stem = Path.ChangeExtension(db, null);
            return Directory.GetFiles(Path.GetDirectoryName(db), Path.GetFileName(stem) + ".*")
                .Where(f => !f.EndsWith(".LCK", StringComparison.OrdinalIgnoreCase))
                .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
                .Select(f => new KeyValuePair<string, byte[]>(f, File.ReadAllBytes(f)))
                .ToList();
        }

        private sealed class SnapshotComparer : IEqualityComparer<KeyValuePair<string, byte[]>>
        {
            public bool Equals(KeyValuePair<string, byte[]> a, KeyValuePair<string, byte[]> b) =>
                a.Key == b.Key && a.Value.SequenceEqual(b.Value);
            public int GetHashCode(KeyValuePair<string, byte[]> k) => k.Key.GetHashCode();
        }

        private static void Check(string name, bool ok)
        {
            if (!ok) failures++;
            Console.WriteLine($"  {(ok ? "PASS" : "FAIL")}: {name}");
        }
    }
}
