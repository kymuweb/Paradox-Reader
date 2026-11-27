using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxTest
{
    /// <summary>
    /// Reusable logic for taking sets of Paradox table files (.DB, .PX, .Xnn/.Ynn, .MB etc. - all files
    /// sharing a table name) and comparing them in code at the byte level, with Paradox-aware
    /// reporting (header field names, per-block diffs, record slots) so differences can be investigated
    /// without resorting to ad-hoc powershell/python scripts.
    /// </summary>
    internal static class BinaryComparisonTest
    {
        private const int BlockHeaderSize = 6;
        private const int MaxBlocksReported = 40;
        private const int MaxRunsPerBlock = 24;
        private const int MaxHeaderDiffsReported = 120;

        // ----------------------------------------------------------------
        // File-set helpers
        // ----------------------------------------------------------------

        /// <summary>
        /// Finds every file belonging to the table (any single extension). A name such as
        /// "ClientVersionLog.bak.DB" is NOT matched for table "ClientVersionLog".
        /// </summary>
        public static List<string> FindTableFiles(string folder, string tableName)
        {
            return Directory.GetFiles(folder, tableName + ".*")
                .Where(f => string.Equals(Path.GetFileNameWithoutExtension(f), tableName, StringComparison.OrdinalIgnoreCase))
                .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        /// <summary>Copies all files of a table set to destFolder, renaming the table part to destTableName.</summary>
        public static List<string> CopyTableSet(string srcFolder, string srcTableName, string destFolder, string destTableName)
        {
            Directory.CreateDirectory(destFolder);
            var copied = new List<string>();
            foreach (var file in FindTableFiles(srcFolder, srcTableName))
            {
                var dest = Path.Combine(destFolder, destTableName + Path.GetExtension(file));
                File.Copy(file, dest, true);
                copied.Add(dest);
            }
            return copied;
        }

        /// <summary>Deletes the files of a table set (used before restoring so no stray files remain).</summary>
        public static void DeleteTableSet(string folder, string tableName)
        {
            foreach (var file in FindTableFiles(folder, tableName))
                File.Delete(file);
        }

        // ----------------------------------------------------------------
        // Comparison
        // ----------------------------------------------------------------

        /// <summary>
        /// Compares two table sets file-by-file (matched by extension). Appends a human readable
        /// report to <paramref name="report"/> and returns true when every file is byte-identical.
        /// </summary>
        public static bool CompareTableSets(string folderA, string tableA, string folderB, string tableB, string labelA, string labelB, StringBuilder report)
        {
            var filesA = FindTableFiles(folderA, tableA).ToDictionary(f => Path.GetExtension(f).ToUpperInvariant());
            var filesB = FindTableFiles(folderB, tableB).ToDictionary(f => Path.GetExtension(f).ToUpperInvariant());
            var extensions = filesA.Keys.Union(filesB.Keys).OrderBy(e => e, StringComparer.Ordinal).ToList();

            report.AppendLine("==============================================================");
            report.AppendLine($"COMPARE  A = {labelA} ({tableA})   B = {labelB} ({tableB})");
            report.AppendLine("==============================================================");

            bool allIdentical = true;
            foreach (var ext in extensions)
            {
                bool hasA = filesA.TryGetValue(ext, out var pathA);
                bool hasB = filesB.TryGetValue(ext, out var pathB);
                if (!hasA || !hasB)
                {
                    allIdentical = false;
                    report.AppendLine($"[{ext}] only present in {(hasA ? "A" : "B")}");
                    continue;
                }

                var a = File.ReadAllBytes(pathA);
                var b = File.ReadAllBytes(pathB);
                if (a.Length == b.Length && BytesEqual(a, b))
                {
                    report.AppendLine($"[{ext}] IDENTICAL ({a.Length} bytes)");
                    continue;
                }

                allIdentical = false;
                DiffFile(ext, a, b, report);
            }

            report.AppendLine(allIdentical ? "RESULT: all files identical" : "RESULT: DIFFERENCES FOUND");
            report.AppendLine();
            return allIdentical;
        }

        private static bool BytesEqual(byte[] a, byte[] b)
        {
            for (int i = 0; i < a.Length; i++)
                if (a[i] != b[i]) return false;
            return true;
        }

        private static void DiffFile(string ext, byte[] a, byte[] b, StringBuilder sb)
        {
            sb.AppendLine($"[{ext}] DIFFERENT   A={a.Length} bytes, B={b.Length} bytes");
            sb.AppendLine("  A header: " + DescribeHeader(a));
            sb.AppendLine("  B header: " + DescribeHeader(b));

            int headerSize = a.Length >= 4 ? BitConverter.ToUInt16(a, 2) : 0;
            int headerSizeB = b.Length >= 4 ? BitConverter.ToUInt16(b, 2) : 0;
            int blockSize = a.Length > 5 ? a[5] * 1024 : 0;
            int recordSize = a.Length >= 2 ? BitConverter.ToUInt16(a, 0) : 0;
            bool isDb = string.Equals(ext, ".DB", StringComparison.OrdinalIgnoreCase);

            // ---- header region -------------------------------------------------
            int hdrLen = Math.Min(Math.Min(headerSize, headerSizeB), Math.Min(a.Length, b.Length));
            if (headerSize != headerSizeB) sb.AppendLine($"  !! header sizes differ: A={headerSize} B={headerSizeB}");
            int shown = 0, totalHdrDiffs = 0;
            for (int i = 0; i < hdrLen; i++)
            {
                if (a[i] == b[i]) continue;
                totalHdrDiffs++;
                if (shown++ < MaxHeaderDiffsReported)
                    sb.AppendLine($"  header 0x{i:X3} ({HeaderLabel(i)}): A={a[i]:X2} B={b[i]:X2}");
            }
            if (totalHdrDiffs > shown) sb.AppendLine($"  ... {totalHdrDiffs - shown} more header byte differences");
            sb.AppendLine($"  header byte differences: {totalHdrDiffs}");

            // ---- block region --------------------------------------------------
            if (blockSize <= 0 || headerSize <= 0)
            {
                sb.AppendLine("  (cannot determine block size; skipping block diff)");
                return;
            }

            int maxLen = Math.Max(a.Length, b.Length);
            int blockCount = (maxLen - headerSize + blockSize - 1) / blockSize;
            int differingBlocks = 0, reportedBlocks = 0;
            for (int blk = 0; blk < blockCount; blk++)
            {
                int start = headerSize + blk * blockSize;
                int lenA = Math.Max(0, Math.Min(blockSize, a.Length - start));
                int lenB = Math.Max(0, Math.Min(blockSize, b.Length - start));
                bool differs = lenA != lenB;
                if (!differs)
                {
                    for (int i = 0; i < lenA; i++)
                        if (a[start + i] != b[start + i]) { differs = true; break; }
                }
                if (!differs) continue;

                differingBlocks++;
                if (reportedBlocks >= MaxBlocksReported) continue;
                reportedBlocks++;

                sb.AppendLine($"  block {blk + 1} (file offset 0x{start:X}):");
                sb.AppendLine("    A: " + DescribeBlockHeader(a, start, lenA, isDb, recordSize));
                sb.AppendLine("    B: " + DescribeBlockHeader(b, start, lenB, isDb, recordSize));
                if (lenA != lenB)
                {
                    sb.AppendLine($"    block present length differs: A={lenA} B={lenB}");
                    continue;
                }

                int runs = 0, pos = 0;
                while (pos < lenA)
                {
                    if (a[start + pos] == b[start + pos]) { pos++; continue; }
                    int runStart = pos;
                    int lastDiff = pos;
                    // merge runs separated by < 4 identical bytes
                    while (pos < lenA && pos - lastDiff < 4)
                    {
                        if (a[start + pos] != b[start + pos]) lastDiff = pos;
                        pos++;
                    }
                    int runLen = lastDiff - runStart + 1;
                    if (runs++ < MaxRunsPerBlock)
                    {
                        int show = Math.Min(runLen, 40);
                        string slot = "";
                        if (isDb && recordSize > 0 && runStart >= BlockHeaderSize)
                            slot = $" [record slot {(runStart - BlockHeaderSize) / recordSize}]";
                        sb.AppendLine($"    +0x{runStart:X4} len {runLen}{slot}");
                        sb.AppendLine($"      A: {BitConverter.ToString(a, start + runStart, show)}{(show < runLen ? " ..." : "")}");
                        sb.AppendLine($"      B: {BitConverter.ToString(b, start + runStart, show)}{(show < runLen ? " ..." : "")}");
                    }
                }
                if (runs > MaxRunsPerBlock) sb.AppendLine($"    ... {runs - MaxRunsPerBlock} more differing runs");
            }
            sb.AppendLine($"  differing blocks: {differingBlocks} of {blockCount}{(differingBlocks > reportedBlocks ? $" (first {reportedBlocks} shown)" : "")}");
        }

        // ----------------------------------------------------------------
        // Decoding helpers
        // ----------------------------------------------------------------

        private static string DescribeHeader(byte[] d)
        {
            if (d.Length < 0x80) return $"(too short: {d.Length} bytes)";
            return $"recSize={BitConverter.ToUInt16(d, 0)} hdrSize={BitConverter.ToUInt16(d, 2)} type={d[4]} blockKB={d[5]} " +
                   $"records={BitConverter.ToUInt32(d, 6)} next={BitConverter.ToUInt16(d, 0x0A)} fileBlocks={BitConverter.ToUInt16(d, 0x0C)} " +
                   $"first={BitConverter.ToUInt16(d, 0x0E)} last={BitConverter.ToUInt16(d, 0x10)} " +
                   $"pxRoot={BitConverter.ToUInt16(d, 0x1E)} pxLevels={d[0x20]} " +
                   $"chg1={d[0x2D]} chg2={d[0x2E]} writeCtr={d[0x2C]} maxBlocks={BitConverter.ToUInt16(d, 0x3A)} " +
                   $"autoInc={BitConverter.ToInt32(d, 0x49)} chg4={BitConverter.ToUInt16(d, 0x70)}";
        }

        private static string DescribeBlockHeader(byte[] d, int start, int len, bool isDb, int recordSize)
        {
            if (len < BlockHeaderSize) return "(block missing/short)";
            int next = BitConverter.ToUInt16(d, start);
            int prev = BitConverter.ToUInt16(d, start + 2);
            int add = BitConverter.ToInt16(d, start + 4);
            string s = $"next={next} prev={prev} addDataSize={add}";
            if (isDb && recordSize > 0 && add >= 0)
                s += $" (=> {add / recordSize + 1} records)";
            return s;
        }

        private static readonly Tuple<int, int, string>[] HeaderFields =
        {
            Tuple.Create(0x00, 2, "RecordSize"),
            Tuple.Create(0x02, 2, "HeaderSize"),
            Tuple.Create(0x04, 1, "FileType"),
            Tuple.Create(0x05, 1, "MaxTableSize"),
            Tuple.Create(0x06, 4, "RecordCount"),
            Tuple.Create(0x0A, 2, "NextBlock"),
            Tuple.Create(0x0C, 2, "FileBlocks"),
            Tuple.Create(0x0E, 2, "FirstBlock"),
            Tuple.Create(0x10, 2, "LastBlock"),
            Tuple.Create(0x1E, 2, "PxRootBlockId"),
            Tuple.Create(0x20, 1, "PxLevelCount"),
            Tuple.Create(0x2C, 1, "WriteCounter"),
            Tuple.Create(0x2D, 1, "ChangeCount1"),
            Tuple.Create(0x2E, 1, "ChangeCount2"),
            Tuple.Create(0x3A, 2, "MaxBlocks"),
            Tuple.Create(0x49, 4, "AutoIncVal"),
            Tuple.Create(0x70, 2, "ChangeCount4"),
            Tuple.Create(0x74, 1, "HasBlobFlag"),
        };

        private static string HeaderLabel(int offset)
        {
            foreach (var f in HeaderFields)
                if (offset >= f.Item1 && offset < f.Item1 + f.Item2)
                    return f.Item3 + (f.Item2 > 1 ? $"+{offset - f.Item1}" : "");
            return "?";
        }
    }
}
