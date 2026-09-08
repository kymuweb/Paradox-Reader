using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ParadoxReader
{
    /// <summary>
    /// Result of a <see cref="TableRebuilder.Rebuild(string, string)"/> operation.
    /// </summary>
    public class TableRebuildResult
    {
        /// <summary>Full path to the rebuilt .DB file (same as the original path).</summary>
        public string TableFilePath { get; internal set; }

        /// <summary>Number of records migrated from the original table into the rebuilt one.</summary>
        public int RecordsMigrated { get; internal set; }

        /// <summary>
        /// Full paths of every associated file (.DB, .PX, .Xnn/.Xgn/.Ynn/.Ygn, .MB)
        /// that was recreated as part of the rebuild.
        /// </summary>
        public List<string> RebuiltFiles { get; internal set; }
    }

    /// <summary>
    /// Implements a Paradox "compact and repair" table rebuild, analogous to
    /// BDE's Pdxrbld utility: reads every record out of an existing table,
    /// recreates the .DB file (and its .PX/secondary-index/.MB companions)
    /// completely from scratch as an empty skeleton with the same schema,
    /// re-inserts every record into the fresh structure (which also rebuilds
    /// every index from nothing, one block at a time, via the normal
    /// insert path), and finally swaps the freshly built files back over
    /// the originals.
    ///
    /// Unlike the ordinary read/write path (<see cref="ParadoxTableFile"/>),
    /// which only ever modifies an already-well-formed table, this class
    /// is the one place responsible for producing a brand-new, empty table
    /// + index skeleton "from scratch". It does this by cloning just the
    /// header bytes of each existing file (preserving schema, field
    /// definitions, table name, sort order, and autoIncVal) and resetting
    /// only the bookkeeping fields that describe the (now empty) data:
    /// RecordCount, the block chain pointers, the PX root block/level
    /// count, the change counters, and maxBlocks. Every other structural
    /// concern (block layout, B-tree construction, blob allocation) is
    /// then handled by the existing, already-battle-tested
    /// <see cref="ParadoxTableFile.InsertRecord"/> / index maintenance /
    /// <see cref="ParadoxBlobFile.WriteBlob"/> code paths, simply by
    /// inserting every record into the empty skeleton in original order.
    /// </summary>
    /// <remarks>
    /// Known limitation: raw BLOb/OLE/Graphic field values reference a slot
    /// in the old .MB file that no longer exists after rebuild (the new .MB
    /// starts out as just its header block), so those references are
    /// cleared rather than carried over as dangling pointers. Memo
    /// (MemoBLOb/FmtMemoBLOb) fields ARE fully preserved: their text is
    /// re-written into the new .MB file via the normal blob-write path.
    /// </remarks>
    public static class TableRebuilder
    {
        private sealed class FilePair
        {
            public string Original { get; }
            public string Temp { get; }
            public FilePair(string original, string temp) { Original = original; Temp = temp; }
        }

        // Physical header offsets are defined once in ParadoxHeaderOffsets and
        // shared across ParadoxTableFile, PrimaryIndexFile, and this class so
        // they can't drift out of sync; see that class for future-version
        // accommodation guidance.

        /// <summary>
        /// Rebuilds (compacts and repairs) the table at <paramref name="dbFilePath"/>,
        /// recreating the .DB file and every associated index/.MB file from
        /// scratch, then atomically replacing the originals.
        /// </summary>
        /// <param name="dbFilePath">Full path to the .DB file to rebuild.</param>
        /// <param name="tempTableName">
        /// Optional base name (without extension) to use for the temporary working
        /// files while the rebuild is in progress (mirroring Pdxrbld's conventional
        /// "RESTTEMP" name). If null, a unique name is generated so concurrent
        /// rebuilds never collide.
        /// </param>
        public static TableRebuildResult Rebuild(string dbFilePath, string tempTableName = null, bool useMemoryStreams = false)
        {
            using (var table = new ParadoxTableFile(dbFilePath))
            {
                return Rebuild(table, tempTableName, useMemoryStreams);
            }
        }

        /// <summary>
        /// Rebuilds (compacts and repairs) <paramref name="table"/>, recreating
        /// its .DB file and every associated index/.MB file from scratch, then
        /// atomically replacing the originals. Takes ownership of
        /// <paramref name="table"/> and disposes it (releasing its file handles
        /// and lock) once its records have been read, so the original files can
        /// be deleted/replaced.
        /// </summary>
        /// <param name="table">The table to rebuild. Disposed by this method.</param>
        /// <param name="tempTableName">
        /// Optional base name (without extension) to use for the temporary working
        /// files while the rebuild is in progress (mirroring Pdxrbld's conventional
        /// "RESTTEMP" name). If null, a unique name is generated so concurrent
        /// rebuilds never collide.
        /// </param>
        /// <param name="useMemoryStreams">
        /// When true, the rebuilt .DB file's contents are staged entirely in
        /// an in-memory <see cref="MemoryStream"/> while every record is
        /// reinserted, and only written back to disk once at the end,
        /// instead of flushing to disk after every insert. This can
        /// significantly speed up rebuilds of large tables at the cost of
        /// holding the whole rebuilt .DB file in memory. Index/blob files
        /// are unaffected and continue to be written directly to disk.
        /// </param>
        public static TableRebuildResult Rebuild(ParadoxTableFile table, string tempTableName = null, bool useMemoryStreams = false)
        {
            if (table == null) throw new ArgumentNullException(nameof(table));

            return RebuildCore(table, tempTableName, newSchema: null, useMemoryStreams);
        }

        /// <summary>
        /// Regenerates <paramref name="table"/> under a new
        /// <paramref name="newSchema"/> (Table > Modify Structure): builds a
        /// fresh empty .DB/.PX/.Xnn skeleton set from
        /// <paramref name="newSchema"/> via <see cref="ParadoxHeaderBuilder"/>
        /// (rather than cloning the existing files' headers), remaps every
        /// existing record's field values onto the new field layout by
        /// matching field names (case-insensitively; fields absent from the
        /// new schema are dropped, new fields default to null, and
        /// differently-typed fields are converted on a best-effort basis,
        /// falling back to null on failure), then re-inserts every record
        /// into the fresh skeleton exactly like <see cref="Rebuild"/> does.
        /// Takes ownership of <paramref name="table"/> and disposes it.
        /// </summary>
        public static TableRebuildResult RebuildWithSchema(ParadoxTableFile table, TableSchemaDefinition newSchema, string tempTableName = null, bool useMemoryStreams = false)
        {
            if (table == null) throw new ArgumentNullException(nameof(table));
            if (newSchema == null) throw new ArgumentNullException(nameof(newSchema));
            if (newSchema.Fields == null || newSchema.Fields.Count == 0)
                throw new ArgumentException("A table must have at least one field.", nameof(newSchema));

            return RebuildCore(table, tempTableName, newSchema, useMemoryStreams);
        }

        private static TableRebuildResult RebuildCore(ParadoxTableFile table, string tempTableName, TableSchemaDefinition newSchema, bool useMemoryStreams = false)
        {
            string dbFilePath = table.FilePath;
            string dir = Path.GetDirectoryName(dbFilePath) ?? ".";
            string baseName = Path.GetFileNameWithoutExtension(dbFilePath);

            // ------------------------------------------------------------
            // 1. Snapshot every record, in on-disk order, before anything
            //    else changes. Enumerate() walks blocks 0..fileBlocks-1 in
            //    order, so insertion order into the rebuilt table exactly
            //    matches the original physical layout.
            // ------------------------------------------------------------
            var records = new List<object[]>();
            foreach (var rec in table.Enumerate())
                records.Add(rec.DataValues);

            var oldFieldTypes = table.FieldTypes;
            var oldFieldNames = table.FieldNames;

            // ------------------------------------------------------------
            // 2. Locate every file belonging to this table (.DB, .PX,
            //    .Xnn/.Xgn/.Ynn/.Ygn, .MB) - anything sharing the exact
            //    base name, excluding the BDE .LCK lock file.
            // ------------------------------------------------------------
            string tempBaseName = string.IsNullOrEmpty(tempTableName)
                ? baseName + "_" + Guid.NewGuid().ToString("N").Substring(0, 8) + "_rbld"
                : tempTableName;

            var sourceFiles = Directory.GetFiles(dir, baseName + ".*")
                .Where(f => Path.GetFileNameWithoutExtension(f).Equals(baseName, StringComparison.OrdinalIgnoreCase))
                .Where(f => !Path.GetExtension(f).Equals(".LCK", StringComparison.OrdinalIgnoreCase))
                .ToArray();

            // ------------------------------------------------------------
            // 3. Build an empty skeleton copy of every associated file
            //    under the temp base name. When newSchema is null (plain
            //    compact-and-repair rebuild), skeletons clone the existing
            //    files' headers with only the "empty data" fields reset.
            //    When newSchema is supplied (Modify Structure), skeletons
            //    are instead synthesized from scratch from newSchema via
            //    ParadoxHeaderBuilder, since the field layout itself is
            //    changing.
            // ------------------------------------------------------------
            var swapPairs = new List<FilePair>();
            string oldMbPath = sourceFiles.FirstOrDefault(f => Path.GetExtension(f).Equals(".MB", StringComparison.OrdinalIgnoreCase));

            // Snapshot changeCount1/changeCount2 (.DB, offset 0x2D/0x2E) and the
            // write counter (.PX/secondary index, offset 0x2C) from every
            // existing file *before* building empty skeletons, so they can be
            // restored verbatim after record reinsertion. See the remarks in
            // CreateEmptyTableSkeleton for why: a clean, single-pass BDE
            // Pdxrbld rebuild leaves these exact bytes unchanged from the
            // pre-rebuild source, rather than resetting/recomputing them, and
            // Paradox 7/SQLRunner reject a rebuilt table whose bytes don't
            // match this expectation ("Index is out of date"). Only applies to
            // the plain compact-and-repair path (newSchema == null); Modify
            // Structure synthesizes brand-new files from scratch instead.
            var preservedChangeCounts = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
            if (newSchema == null)
            {
                foreach (var src in sourceFiles)
                {
                    string ext = Path.GetExtension(src);
                    if (ext.Equals(".MB", StringComparison.OrdinalIgnoreCase))
                        continue;

                    byte[] header = ReadHeaderBytes(src);
                    if (ext.Equals(".DB", StringComparison.OrdinalIgnoreCase))
                        preservedChangeCounts[ext] = new[] { header[ParadoxHeaderOffsets.ChangeCount1], header[ParadoxHeaderOffsets.ChangeCount2] };
                    else
                        preservedChangeCounts[ext] = new[] { header[ParadoxHeaderOffsets.WriteCounter] };
                }
            }

            if (newSchema == null)
            {
                foreach (var src in sourceFiles)
                {
                    string ext = Path.GetExtension(src);
                    string dest = Path.Combine(dir, tempBaseName + ext);

                    if (ext.Equals(".MB", StringComparison.OrdinalIgnoreCase))
                        CreateEmptyBlobSkeleton(src, dest);
                    else
                        CreateEmptyTableSkeleton(src, dest);

                    swapPairs.Add(new FilePair(src, dest));
                }
            }
            else
            {
                string tempDbDest = Path.Combine(dir, tempBaseName + ".DB");
                File.WriteAllBytes(tempDbDest, ParadoxHeaderBuilder.BuildDbHeader(newSchema));
                swapPairs.Add(new FilePair(dbFilePath, tempDbDest));

                string oldPxPath = sourceFiles.FirstOrDefault(f => Path.GetExtension(f).Equals(".PX", StringComparison.OrdinalIgnoreCase));
                if (newSchema.PrimaryKeyFieldCount > 0)
                {
                    string tempPxDest = Path.Combine(dir, tempBaseName + ".PX");
                    File.WriteAllBytes(tempPxDest, ParadoxHeaderBuilder.BuildPxHeader(newSchema));
                    swapPairs.Add(new FilePair(oldPxPath ?? Path.ChangeExtension(dbFilePath, ".PX"), tempPxDest));
                }
                else if (oldPxPath != null)
                {
                    // Primary key removed entirely: the old .PX no longer applies.
                    File.Delete(oldPxPath);
                }

                // Existing secondary index files (.Xnn/.Xgn/.Ynn/.Ygn) don't map
                // cleanly onto a changed field layout, so they're simply removed;
                // the caller is expected to have captured/re-specified the desired
                // indexes in newSchema.Indexes, built fresh below.
                foreach (var src in sourceFiles)
                {
                    string ext = Path.GetExtension(src).ToUpperInvariant();
                    if (ext.Length >= 2 && (ext[1] == 'X' || ext[1] == 'Y') && !ext.Equals(".PX", StringComparison.OrdinalIgnoreCase))
                        File.Delete(src);
                }

                // Every index this library creates corresponds to a named
                // index (equivalent to SQLRunner's CREATE INDEX), which real
                // BDE always names .XGn/.YGn with a sequential ordinal - see
                // ParadoxHeaderBuilder.BuildSecondaryIndexHeader remarks.
                int indexOrdinal = 0;
                foreach (var index in newSchema.Indexes)
                {
                    string ordinal = (indexOrdinal++).ToString();
                    string xExt = ".XG" + ordinal;
                    string yExt = ".YG" + ordinal;

                    string tempIndexDest = Path.Combine(dir, tempBaseName + xExt);
                    File.WriteAllBytes(tempIndexDest, ParadoxHeaderBuilder.BuildSecondaryIndexHeader(newSchema, index));
                    swapPairs.Add(new FilePair(Path.ChangeExtension(dbFilePath, xExt), tempIndexDest));

                    string tempYDest = Path.Combine(dir, tempBaseName + yExt);
                    File.WriteAllBytes(tempYDest, ParadoxHeaderBuilder.BuildMaintainedFieldHeader(newSchema, index));
                    swapPairs.Add(new FilePair(Path.ChangeExtension(dbFilePath, yExt), tempYDest));
                }

                bool hasBlobField = newSchema.Fields.Any(f =>
                    f.Type == ParadoxFieldTypes.MemoBLOb || f.Type == ParadoxFieldTypes.FmtMemoBLOb ||
                    f.Type == ParadoxFieldTypes.BLOb || f.Type == ParadoxFieldTypes.OLE || f.Type == ParadoxFieldTypes.Graphic);
                if (hasBlobField && oldMbPath != null)
                {
                    string tempMbDest = Path.Combine(dir, tempBaseName + ".MB");
                    CreateEmptyBlobSkeleton(oldMbPath, tempMbDest);
                    swapPairs.Add(new FilePair(oldMbPath, tempMbDest));
                }
                else if (!hasBlobField && oldMbPath != null)
                {
                    File.Delete(oldMbPath);
                }
            }

            string tempDbPath = Path.Combine(dir, tempBaseName + ".DB");

            // ------------------------------------------------------------
            // 4. Re-insert every record into the fresh skeleton. This
            //    single step rebuilds the block chain, every open index's
            //    B-tree (via the normal IndexManager.OnBlockChanged path
            //    inside InsertRecord), and every memo's .MB storage (via
            //    the normal WriteBlob path) completely from scratch.
            // ------------------------------------------------------------
            int migrated = 0;

            // When useMemoryStreams is enabled, the temp .DB file's bytes are
            // loaded into a MemoryStream up front, every record is inserted
            // against that in-memory copy (avoiding a disk flush per insert -
            // see BlockManager.WriteBlock/ParadoxTableFile.WriteRecordCountToHeader),
            // and the final contents are written back to disk exactly once
            // after every record has been migrated.
            MemoryStream memDbStream = null;
            Dictionary<string, MemoryStream> memStreamsByPath = null;
            ParadoxTableFile newTable;
            if (useMemoryStreams)
            {
                // Load every temp skeleton file (.DB, .PX, .Xgn/.Ygn, .MB)
                // created above into its own expandable MemoryStream, keyed
                // by full path, so ParadoxTableFile/IndexManager/
                // ParadoxBlobFile can operate on them entirely in memory
                // during reinsertion instead of hitting disk per write.
                memStreamsByPath = new Dictionary<string, MemoryStream>(StringComparer.OrdinalIgnoreCase);
                foreach (var pair in swapPairs)
                    memStreamsByPath[pair.Temp] = LoadExpandableMemoryStream(pair.Temp);

                memDbStream = memStreamsByPath[tempDbPath];
                var associatedStreams = new Dictionary<string, Stream>(StringComparer.OrdinalIgnoreCase);
                foreach (var kvp in memStreamsByPath)
                {
                    if (!kvp.Key.Equals(tempDbPath, StringComparison.OrdinalIgnoreCase))
                        associatedStreams[kvp.Key] = kvp.Value;
                }

                newTable = new ParadoxTableFile(memDbStream, tempDbPath, associatedStreams);
            }
            else
            {
                newTable = new ParadoxTableFile(tempDbPath);
            }
            try
            {
                var newFieldTypes = newTable.FieldTypes;

                // Hold the .LCK write lock for the entire reinsertion loop
                // instead of letting each InsertRecord acquire/release it
                // individually. This is always correct (no other process
                // can be using the brand-new temp table yet) and, when
                // useMemoryStreams is enabled, avoids writing/deleting the
                // .LCK file once per record while every other write is
                // already happening purely in memory.
                using (newTable.AcquireScopedBatchWriteLock())
                {
                    foreach (var original in records)
                    {
                        object[] values = newSchema == null
                            ? (object[])original.Clone()
                            : RemapValues(original, oldFieldNames, oldFieldTypes, newSchema);

                        ClearStaleBlobReferences(values, newFieldTypes);
                        newTable.InsertRecord(values);
                        migrated++;
                    }
                }
            }
            finally
            {
                if (memStreamsByPath != null)
                {
                    // Persist every in-memory temp artifact back to disk
                    // exactly once, now that every record has been migrated.
                    foreach (var kvp in memStreamsByPath)
                        File.WriteAllBytes(kvp.Key, kvp.Value.ToArray());
                }

                newTable.Dispose();
            }

            // ------------------------------------------------------------
            // 4b. Restore the pre-rebuild changeCount1/changeCount2 (.DB)
            //     and write counter (.PX/secondary index) bytes captured in
            //     step 3, overwriting whatever value the normal per-insert
            //     increment path above left behind. See the snapshot capture
            //     above and CreateEmptyTableSkeleton's remarks for why.
            // ------------------------------------------------------------
            if (newSchema == null)
            {
                foreach (var pair in swapPairs)
                {
                    string ext = Path.GetExtension(pair.Temp);
                    if (!preservedChangeCounts.TryGetValue(ext, out var saved))
                        continue;

                    using (var fs = new FileStream(pair.Temp, FileMode.Open, FileAccess.Write, FileShare.None))
                    {
                        if (ext.Equals(".DB", StringComparison.OrdinalIgnoreCase))
                        {
                            fs.Position = ParadoxHeaderOffsets.ChangeCount1;
                            fs.WriteByte(saved[0]);
                            fs.Position = ParadoxHeaderOffsets.ChangeCount2;
                            fs.WriteByte(saved[1]);
                        }
                        else
                        {
                            fs.Position = ParadoxHeaderOffsets.WriteCounter;
                            fs.WriteByte(saved[0]);
                        }
                    }
                }
            }

            // ------------------------------------------------------------
            // 5. Release the original table's file handles/lock so its
            //    files can be deleted, then atomically swap the rebuilt
            //    files back over the originals.
            // ------------------------------------------------------------
            table.Dispose();

            foreach (var pair in swapPairs)
            {
                if (File.Exists(pair.Original))
                {
                    try
                    {
                        File.Delete(pair.Original);
                    }
                    catch
                    {
                        var origFolderPath = Path.GetDirectoryName(pair.Original);
                        var origFileNameNoExt = Path.GetFileNameWithoutExtension(pair.Original);
                        var origFileExt = Path.GetExtension(pair.Original);
                        var tempOrigFileName = origFileNameNoExt + "_old_" + Guid.NewGuid().ToString("N").Substring(0, 8) + origFileExt;
                        var tempOrigFilePath = Path.Combine(origFolderPath, tempOrigFileName);
                        try
                        {
                            File.Move(pair.Original, tempOrigFilePath); // If we can't delete the original, move it aside so the new file can be moved into place.
                        }
                        catch (Exception moveAsideEx)
                        {
                            // Couldn't delete AND couldn't even move the original aside (e.g. still
                            // locked by another process). There's no safe way to proceed: some file
                            // pairs in this rebuild may already have been swapped while others
                            // (including this one) were not, so stop immediately rather than risk
                            // silently leaving the table in a half-rebuilt, inconsistent state.
                            throw new IOException(
                                $"Table rebuild aborted: could not delete or move aside the original " +
                                $"file '{pair.Original}' (still in use?). The rebuild may be left in a " +
                                $"partially-swapped state; re-run the rebuild once the file is no longer " +
                                $"locked.", moveAsideEx);
                        }
                        TempFilePathsToBeDeletedLater?.Add(tempOrigFilePath); // Orphaned leftover — track it for later cleanup since we couldn't delete it now.
                    }
                }

                try
                {
                    // We weren't always able to move the original because "The process cannot access the file because it is being used by another process."
                    File.Move(pair.Temp, pair.Original);
                }
                catch
                {
                    TempFilePathsToBeDeletedLater?.Add(pair.Temp); // A bit of a hack, but if we can't move the temp file over the original, just copy it and leave the temp file behind for later cleanup.
                    File.Copy(pair.Temp, pair.Original, overwrite: true);
                }
            }


            return new TableRebuildResult
            {
                TableFilePath = dbFilePath,
                RecordsMigrated = migrated,
                RebuiltFiles = swapPairs.Select(p => p.Original).ToList()
            };
        }

        public static List<string> TempFilePathsToBeDeletedLater = new List<string>(); // TODO: handle these later or just don't worry?

        /// <summary>
        /// Builds a new record's values for <paramref name="newSchema"/> from
        /// an old record's values, matching fields by name
        /// (case-insensitive). Fields with no matching old field become
        /// null; fields whose old and new types differ are converted via
        /// <see cref="Convert.ChangeType(object, Type)"/> where possible,
        /// falling back to null if the value can't be converted.
        /// </summary>
        private static object[] RemapValues(
            object[] oldValues, string[] oldFieldNames, ParadoxFile.FieldInfo[] oldFieldTypes, TableSchemaDefinition newSchema)
        {
            var newValues = new object[newSchema.Fields.Count];

            for (int newIndex = 0; newIndex < newSchema.Fields.Count; newIndex++)
            {
                var newField = newSchema.Fields[newIndex];
                int oldIndex = Array.FindIndex(oldFieldNames,
                    n => string.Equals(n, newField.Name, StringComparison.OrdinalIgnoreCase));
                if (oldIndex < 0 || oldIndex >= oldValues.Length)
                    continue;

                object oldValue = oldValues[oldIndex];
                if (oldValue == null)
                    continue;

                if (oldFieldTypes[oldIndex].fType == newField.Type)
                {
                    newValues[newIndex] = oldValue;
                    continue;
                }

                newValues[newIndex] = TryConvertValue(oldValue, newField.Type);
            }

            return newValues;
        }

        private static object TryConvertValue(object value, ParadoxFieldTypes targetType)
        {
            try
            {
                switch (targetType)
                {
                    case ParadoxFieldTypes.Alpha:
                        return Convert.ToString(value);
                    case ParadoxFieldTypes.Short:
                        return Convert.ToInt16(value);
                    case ParadoxFieldTypes.Long:
                    case ParadoxFieldTypes.AutoInc:
                        return Convert.ToInt32(value);
                    case ParadoxFieldTypes.Currency:
                    case ParadoxFieldTypes.Number:
                        return Convert.ToDouble(value);
                    case ParadoxFieldTypes.Logical:
                        return Convert.ToBoolean(value);
                    case ParadoxFieldTypes.Date:
                    case ParadoxFieldTypes.Time:
                    case ParadoxFieldTypes.Timestamp:
                        return Convert.ToDateTime(value);
                    default:
                        // Memo/blob/bytes/BCD and other complex types aren't
                        // safely convertible from an arbitrary source type.
                        return null;
                }
            }
            catch
            {
                return null;
            }
        }

        // ----------------------------------------------------------------
        // Skeleton construction
        // ----------------------------------------------------------------

        /// <summary>
        /// Clones just the header portion of a .DB/.PX/.Xnn/.Xgn/.Ynn/.Ygn
        /// file (preserving schema, field definitions, table name, sort
        /// order, and its own existing autoIncVal) and resets the fields
        /// that describe its (now empty) data: RecordCount, block chain
        /// pointers, PX root block id/level count, change counters, and
        /// maxBlocks.
        /// </summary>
        private static void CreateEmptyTableSkeleton(string srcPath, string destPath)
        {
            byte[] header = ReadHeaderBytes(srcPath);

            var fileType = (ParadoxFileType)header[ParadoxHeaderOffsets.FileType];
            bool isSecondaryIndex =
                fileType == ParadoxFileType.XnnFileNonInc || fileType == ParadoxFileType.XnnFileInc ||
                fileType == ParadoxFileType.YnnFile ||
                fileType == ParadoxFileType.XgnFileNonInc || fileType == ParadoxFileType.XgnFileInc ||
                fileType == ParadoxFileType.YgnFile;

            ZeroRegion(header, ParadoxHeaderOffsets.RecordCount, 4);
            ZeroRegion(header, ParadoxHeaderOffsets.BlockChain, 8); // nextBlock+fileBlocks+firstBlock+lastBlock
            ZeroRegion(header, ParadoxHeaderOffsets.PxRootBlockId, 2);
            ZeroRegion(header, ParadoxHeaderOffsets.PxLevelCount, 1);
            // NOTE: changeCount1/changeCount2 (0x2D/0x2E) and the .PX/secondary
            // index writeCounter (0x2C) are deliberately NOT zeroed here anymore.
            // Binary-search bisection against a minimal single-PK-only repro
            // table (no secondary indexes) proved these bytes are the actual
            // discriminating fields behind Paradox 7/SQLRunner's "Index is out
            // of date" rejection of our rebuilt tables: a clean, single-pass
            // BDE Pdxrbld rebuild of the exact same source file leaves its .DB
            // changeCount1/changeCount2 and its .PX writeCounter completely
            // UNCHANGED from the pre-rebuild source (verified byte-for-byte
            // identical, and reproducible across repeated independent Pdxrbld
            // runs on the same input). Our previous implementation zeroed
            // these fields and let the normal per-record-insert increment path
            // (ParadoxTableFile.IncrementChangeCount /
            // PrimaryIndexFile.IncrementWriteCounter /
            // SecondaryIndexFile.IncrementWriteCounter) recompute them from 0
            // during the rebuild's record-reinsertion loop, which produced a
            // value equal to the migrated record count instead of the
            // preserved original value - this is corrected below in
            // RebuildCore, which snapshots these exact bytes before the
            // rebuild and restores them verbatim afterward once every record
            // has been re-inserted.
            ZeroRegion(header, ParadoxHeaderOffsets.MaxBlocks, 2);

            // autoIncVal (@0x49) semantics were re-derived via a 4-case
            // SQLRunner "oracle" matrix (SELECT COUNT(*) via SQLRunner
            // returns exactly 1 row when the index is structurally valid,
            // and silently falls back to a degenerate scan returning N rows
            // when it is not) covering: single-field INTEGER PK + secondary
            // index, composite 2-field INTEGER PK, AUTOINC PK + two
            // secondary indexes, and no PK (no .PX) + one secondary index.
            // Every case with a .PX file failed real BDE/Paradox 7
            // validation ("Index is out of date") after this rebuild left
            // its pre-existing, stale autoIncVal byte value untouched in
            // the skeleton. Real BDE's own Pdxrbld rebuild was independently
            // confirmed (across all three .PX-bearing cases, regardless of
            // PK shape or whether the table even had an AutoInc field) to
            // always write exactly 1 into .PX's autoIncVal, so that is
            // reproduced explicitly below. Secondary index
            // (.Xnn/.Xgn/.Ynn/.Ygn) autoIncVal is left as whatever the clone
            // carried over: every record is re-inserted into this skeleton
            // via the normal ParadoxTableFile.InsertRecord path, which calls
            // IndexManager.SyncAutoIncVal(...) itself whenever an AutoInc
            // field is assigned (see ParadoxTableFile.AssignAutoIncValues),
            // so those files' autoIncVal ends up correctly reflecting their
            // own real usage regardless of this skeleton's starting value.
            if (fileType == ParadoxFileType.PxFile)
                Array.Copy(BitConverter.GetBytes(1), 0, header, ParadoxHeaderOffsets.AutoIncVal, 4);

            // changeCount4 (V4Hdr) only physically exists when the header
            // region is large enough to contain it.
            if (header.Length >= ParadoxHeaderOffsets.ChangeCount4 + 2)
                ZeroRegion(header, ParadoxHeaderOffsets.ChangeCount4, 2);

            if (isSecondaryIndex)
            {
                // BDE/SQLRunner always creates a fresh secondary index (.Xnn/.Xgn/
                // .Ynn/.Ygn) file with one pre-allocated (but empty) root block -
                // the on-disk file is never just the bare header. Empirically
                // confirmed against SQLRunner-created FRESHRBLD.XG0/.YG0: the
                // block-chain fields (nextBlock/fileBlocks/firstBlock/lastBlock)
                // are all 1, maxBlocks is 1, pxRootBlockId is the file's own
                // block-numbering base (0 for XgnFile* types, 1 otherwise), and
                // the block itself is the same "empty root" sentinel that
                // SecondaryIndexFile.AllocateBlock/WriteBlock already reuses on
                // the very first insert (see SecondaryIndexFile.OnBlockChanged).
                // Cloning only the header (as done for .DB/.PX) leaves the
                // rebuilt index 2048 bytes short of this and structurally
                // incompatible with BDE, even though our own reader never
                // required the extra block to function.
                ushort blockBase = (fileType == ParadoxFileType.XgnFileNonInc || fileType == ParadoxFileType.XgnFileInc)
                    ? (ushort)0 : (ushort)1;
                int blockSize = header[ParadoxHeaderOffsets.MaxTableSize] * 0x400;

                header[ParadoxHeaderOffsets.BlockChain] = 1;     // nextBlock
                header[ParadoxHeaderOffsets.BlockChain + 2] = 1; // fileBlocks
                header[ParadoxHeaderOffsets.BlockChain + 4] = 1; // firstBlock
                header[ParadoxHeaderOffsets.BlockChain + 6] = 1; // lastBlock
                Array.Copy(BitConverter.GetBytes(blockBase), 0, header, ParadoxHeaderOffsets.PxRootBlockId, 2);
                Array.Copy(BitConverter.GetBytes((ushort)1), 0, header, ParadoxHeaderOffsets.MaxBlocks, 2);

                var rootBlock = new byte[blockSize];
                // Empty-root-block sentinel: leftChild=0, reserved=0,
                // usedBytes=0xFFF8 (observed verbatim in SQLRunner-created
                // empty secondary indexes; overwritten with real entry data
                // by SecondaryIndexFile.WriteBlock on first insert).
                rootBlock[4] = 0xF8;
                rootBlock[5] = 0xFF;

                using (var fs = new FileStream(destPath, FileMode.Create, FileAccess.Write))
                {
                    fs.Write(header, 0, header.Length);
                    fs.Write(rootBlock, 0, rootBlock.Length);
                }
                return;
            }

            File.WriteAllBytes(destPath, header);
        }

        /// <summary>
        /// Clones just the first (header) 4096-byte block of a .MB blob
        /// file and resets its global modification counter, giving a fresh
        /// blob file with no allocated data blocks - matching the state a
        /// freshly created table's .MB file would be in before any memo was
        /// ever written.
        /// </summary>
        private static void CreateEmptyBlobSkeleton(string srcPath, string destPath)
        {
            byte[] header = new byte[ParadoxHeaderOffsets.BlobHeaderBlockSize];
            using (var fs = new FileStream(srcPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                int read = 0;
                while (read < ParadoxHeaderOffsets.BlobHeaderBlockSize)
                {
                    int n = fs.Read(header, read, ParadoxHeaderOffsets.BlobHeaderBlockSize - read);
                    if (n <= 0) break;
                    read += n;
                }
            }

            ZeroRegion(header, ParadoxHeaderOffsets.BlobModCounter, 2);

            File.WriteAllBytes(destPath, header);
        }

        private static byte[] ReadHeaderBytes(string path)
        {
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var r = new BinaryReader(fs))
            {
                fs.Position = ParadoxHeaderOffsets.HeaderSize;
                int headerSize = r.ReadUInt16();
                fs.Position = 0;
                return r.ReadBytes(headerSize);
            }
        }

        /// <summary>
        /// Reads all bytes of <paramref name="path"/> into a new,
        /// expandable <see cref="MemoryStream"/> (rewound to position 0).
        /// <see cref="MemoryStream(byte[])"/> is deliberately avoided since
        /// it produces a fixed-size, non-expandable buffer that throws
        /// "Memory stream is not expandable" once a caller (e.g.
        /// <see cref="BlockManager"/> allocating a new block) needs to grow
        /// it past its initial length.
        /// </summary>
        private static MemoryStream LoadExpandableMemoryStream(string path)
        {
            byte[] initialBytes = File.ReadAllBytes(path);
            var ms = new MemoryStream(initialBytes.Length);
            ms.Write(initialBytes, 0, initialBytes.Length);
            ms.Position = 0;
            return ms;
        }

        private static void ZeroRegion(byte[] data, int offset, int length)
        {
            for (int i = 0; i < length; i++)
                data[offset + i] = 0;
        }

        // ----------------------------------------------------------------
        // Record migration helpers
        // ----------------------------------------------------------------

        /// <summary>
        /// Memo (MemoBLOb/FmtMemoBLOb) fields carry a BlobInfo reference into
        /// the OLD .MB file; zeroing it makes <see cref="KeySerializer"/>/
        /// <see cref="ParadoxBlobFile.WriteBlob"/> treat the value as a
        /// brand-new write into the rebuilt .MB file, correctly re-persisting
        /// the memo's text. Raw BLOb/OLE/Graphic fields only carry an opaque
        /// pointer (no text to preserve here), and that pointer would
        /// otherwise dangle into the truncated old .MB file, so it is
        /// cleared instead of copied.
        /// </summary>
        private static void ClearStaleBlobReferences(object[] values, ParadoxFile.FieldInfo[] fieldTypes)
        {
            for (int i = 0; i < fieldTypes.Length && i < values.Length; i++)
            {
                var field = fieldTypes[i];
                switch (field.fType)
                {
                    case ParadoxFieldTypes.MemoBLOb:
                    case ParadoxFieldTypes.FmtMemoBLOb:
                        if (values[i] is MemoValue mv)
                            values[i] = new MemoValue(mv.Text, new byte[field.fSize]);
                        break;

                    case ParadoxFieldTypes.BLOb:
                    case ParadoxFieldTypes.OLE:
                    case ParadoxFieldTypes.Graphic:
                        values[i] = null;
                        break;
                }
            }
        }
    }
}
