using System;
using System.Collections.Generic;
using System.IO;

namespace ParadoxReader
{
    /// <summary>
    /// Scans the directory of a .DB file for all secondary index files
    /// (.Xnn, .Xgn) and reads their headers to determine which fields
    /// they index.
    /// </summary>
    internal static class SecondaryIndexDiscovery
    {
        public static List<SecondaryIndexInfo> Discover(
            string dbFilePath, ParadoxFile.FieldInfo[] allFields, int primaryKeyFieldCount)
        {
            var    result = new List<SecondaryIndexInfo>();
            string dir    = Path.GetDirectoryName(dbFilePath) ?? ".";
            string name   = Path.GetFileNameWithoutExtension(dbFilePath);

            // Non-incremental secondary indexes: .X00 - .X99
            TryScanPattern(dir, name, "X",  padWidth: 2, from: 0, to: 99,  allFields, primaryKeyFieldCount, result);

            // Incremental secondary indexes: .XG0 - .XG9
            TryScanPattern(dir, name, "XG", padWidth: 1, from: 0, to: 9,   allFields, primaryKeyFieldCount, result);

            // Maintained-field companions to non-incremental indexes: .Y00 - .Y99
            TryScanPattern(dir, name, "Y",  padWidth: 2, from: 0, to: 99,  allFields, primaryKeyFieldCount, result);

            // Maintained-field companions to incremental indexes: .YG0 - .YG9
            TryScanPattern(dir, name, "YG", padWidth: 1, from: 0, to: 9,   allFields, primaryKeyFieldCount, result);

            return result;
        }

        private static void TryScanPattern(
            string dir, string baseName, string prefix,
            int padWidth, int from, int to,
            ParadoxFile.FieldInfo[] allFields, int primaryKeyFieldCount, List<SecondaryIndexInfo> result)
        {
            for (int i = from; i <= to; i++)
            {
                string ext  = $".{prefix}{i.ToString().PadLeft(padWidth, '0')}";
                string path = Path.Combine(dir, baseName + ext);
                if (!File.Exists(path)) continue;

                try
                {
                    var info = ReadIndexHeader(path, allFields, primaryKeyFieldCount);
                    if (info != null) result.Add(info);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine(
                        $"[SecondaryIndexDiscovery] Skipping {path}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Opens an index file and reads its header to determine which
        /// parent-table fields it indexes. Uses indexFieldNumber from the
        /// header (1-based field position in the parent table) for an exact
        /// mapping, falling back to type+size matching only when it is zero.
        /// </summary>
        private static SecondaryIndexInfo ReadIndexHeader(
            string indexFilePath, ParadoxFile.FieldInfo[] allFields, int primaryKeyFieldCount)
        {
            using (var f = new ParadoxFile(indexFilePath))
            {
                if (f.FieldCount <= 0 || f.FieldTypes == null)
                    return null;

                var indexedFields = new List<ParadoxFile.FieldInfo>();
                var fieldIndices  = new List<int>();

                int startField = f.indexFieldNumber - 1; // convert 1-based → 0-based

                if (startField >= 0 && startField < allFields.Length)
                {
                    // Primary path: use the explicit field number from the header
                    // to locate the FIRST indexed field. The secondary index's
                    // own field list is composed of: the indexed field(s),
                    // followed by the parent table's primary key field(s)
                    // (appended for uniqueness), followed by one trailing
                    // non-key field (observed to always be a 2-byte Short)
                    // that Paradox appends to every .XGn index and is neither
                    // an indexed field nor a PK field. That trailing field is
                    // never present on .YGn companions (which also report
                    // indexFieldNumber=0/primaryKeyFields=0 and take the
                    // fallback path below instead).
                    //
                    // So: totalKeyFields = FieldCount - 1 (drop the trailing
                    // field) covers indexed + PK fields, some of which may
                    // overlap (an indexed field can itself be one of the PK
                    // fields, e.g. a secondary sort order over a compound
                    // key). indexedFieldCount is therefore the portion of
                    // totalKeyFields beyond the PK fields, with a floor of 1
                    // since the header's indexFieldNumber always identifies
                    // at least one indexed field even when it fully overlaps
                    // the primary key.
                    int totalKeyFields    = f.FieldCount - 1;
                    int indexedFieldCount = Math.Max(1, totalKeyFields - primaryKeyFieldCount);

                    // Composite indexes (indexedFieldCount > 1) are not
                    // guaranteed to cover contiguous parent-table field
                    // positions (e.g. CREATE INDEX ... (INTVAL, DATEVAL) with
                    // other columns between them), so match each subsequent
                    // indexed field by type+size, searching forward from just
                    // after the previous match rather than assuming (startField + i).
                    int searchFrom = startField;
                    for (int i = 0; i < indexedFieldCount; i++)
                    {
                        var idxField = f.FieldTypes[i];
                        int foundPos = -1;
                        for (int j = searchFrom; j < allFields.Length; j++)
                        {
                            if (allFields[j].fType == idxField.fType && allFields[j].fSize == idxField.fSize)
                            {
                                foundPos = j;
                                break;
                            }
                        }

                        // Fall back to the contiguous assumption if no forward
                        // type+size match was found (e.g. ambiguous/ordering
                        // edge cases not covered by the composite scan above).
                        if (foundPos < 0) foundPos = startField + i;
                        if (foundPos >= allFields.Length) break;

                        indexedFields.Add(allFields[foundPos]);
                        fieldIndices.Add(foundPos);
                        searchFrom = foundPos + 1;
                    }

                    // Append the primary key fields (positions 0..primaryKeyFieldCount-1)
                    // so the composed key matches what Paradox stores on disk, avoiding
                    // duplicate-key collisions and preventing later fields from being
                    // misinterpreted as extra indexed columns.
                    for (int i = 0; i < primaryKeyFieldCount && i < allFields.Length; i++)
                    {
                        if (fieldIndices.Contains(i)) continue;
                        indexedFields.Add(allFields[i]);
                        fieldIndices.Add(i);
                    }
                }
                else
                {
                    // Fallback: match by type + size (ambiguous when fields share both).
                    System.Diagnostics.Debug.WriteLine(
                        $"[SecondaryIndexDiscovery] '{indexFilePath}': indexFieldNumber={f.indexFieldNumber} " +
                        $"is out of range (allFields.Length={allFields.Length}), falling back to type+size match.");

                    foreach (var idxField in f.FieldTypes)
                    {
                        for (int j = 0; j < allFields.Length; j++)
                        {
                            if (allFields[j].fType == idxField.fType &&
                                allFields[j].fSize == idxField.fSize)
                            {
                                indexedFields.Add(allFields[j]);
                                fieldIndices.Add(j);
                                break;
                            }
                        }
                    }
                }

                if (indexedFields.Count == 0)
                {
                    System.Diagnostics.Debug.WriteLine(
                        $"[SecondaryIndexDiscovery] '{indexFilePath}': could not map any fields — skipping.");
                    return null;
                }

                System.Diagnostics.Debug.WriteLine(
                    $"[SecondaryIndexDiscovery] '{indexFilePath}': mapped {indexedFields.Count} field(s), " +
                    $"indices=[{string.Join(",", fieldIndices.ConvertAll(i => i.ToString()).ToArray())}]");

                return new SecondaryIndexInfo
                {
                    FilePath      = indexFilePath,
                    IndexedFields = indexedFields.ToArray(),
                    FieldIndices  = fieldIndices.ToArray()
                };
            }
        }
    }
}