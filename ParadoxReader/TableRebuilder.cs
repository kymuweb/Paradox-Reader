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
        public static TableRebuildResult Rebuild(string dbFilePath, string tempTableName = null)
        {
            using (var table = new ParadoxTableFile(dbFilePath))
            {
                return Rebuild(table, tempTableName);
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
        public static TableRebuildResult Rebuild(ParadoxTableFile table, string tempTableName = null)
        {
            if (table == null) throw new ArgumentNullException(nameof(table));

            return RebuildCore(table, tempTableName, newSchema: null);
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
        public static TableRebuildResult RebuildWithSchema(ParadoxTableFile table, TableSchemaDefinition newSchema, string tempTableName = null)
        {
            if (table == null) throw new ArgumentNullException(nameof(table));
            if (newSchema == null) throw new ArgumentNullException(nameof(newSchema));
            if (newSchema.Fields == null || newSchema.Fields.Count == 0)
                throw new ArgumentException("A table must have at least one field.", nameof(newSchema));

            return RebuildCore(table, tempTableName, newSchema);
        }

        private static TableRebuildResult RebuildCore(ParadoxTableFile table, string tempTableName, TableSchemaDefinition newSchema)
        {
            string dbFilePath = table.FilePath;
            string dir        = Path.GetDirectoryName(dbFilePath) ?? ".";
            string baseName   = Path.GetFileNameWithoutExtension(dbFilePath);

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

            if (newSchema == null)
            {
                foreach (var src in sourceFiles)
                {
                    string ext  = Path.GetExtension(src);
                    string dest = Path.Combine(dir, tempBaseName + ext);

                    if (ext.Equals(".MB", StringComparison.OrdinalIgnoreCase))
                        CreateEmptyBlobSkeleton(src, dest);
                    else
                        CreateEmptyTableSkeleton(src, dest, table.autoIncVal);

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

                int indexOrdinal = 0;
                foreach (var index in newSchema.Indexes)
                {
                    string ext = ".X" + (indexOrdinal++).ToString().PadLeft(2, '0');
                    string tempIndexDest = Path.Combine(dir, tempBaseName + ext);
                    File.WriteAllBytes(tempIndexDest, ParadoxHeaderBuilder.BuildSecondaryIndexHeader(newSchema, index));
                    swapPairs.Add(new FilePair(Path.ChangeExtension(dbFilePath, ext), tempIndexDest));
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
            using (var newTable = new ParadoxTableFile(tempDbPath))
            {
                var newFieldTypes = newTable.FieldTypes;
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

            // ------------------------------------------------------------
            // 5. Release the original table's file handles/lock so its
            //    files can be deleted, then atomically swap the rebuilt
            //    files back over the originals.
            // ------------------------------------------------------------
            table.Dispose();

            foreach (var pair in swapPairs)
            {
                if (File.Exists(pair.Original))
                    File.Delete(pair.Original);

                File.Move(pair.Temp, pair.Original);
            }

            return new TableRebuildResult
            {
                TableFilePath   = dbFilePath,
                RecordsMigrated = migrated,
                RebuiltFiles    = swapPairs.Select(p => p.Original).ToList()
            };
        }

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
        /// order, and autoIncVal) and resets the fields that describe its
        /// (now empty) data: RecordCount, block chain pointers, PX root
        /// block id/level count, change counters, and maxBlocks.
        /// </summary>
        /// <param name="autoIncVal">
        /// The parent .DB's current autoIncVal, forced into every skeleton's
        /// header (overriding whatever value the source index file itself
        /// had) so the freshly rebuilt .DB/.PX/secondary-index set all agree
        /// from the outset. Without this, an index that was out of date
        /// before the rebuild (the very case TableRebuilder exists to fix)
        /// would carry its stale autoIncVal straight into the rebuilt
        /// skeleton, making IndexManager/ParadoxPrimaryKey immediately flag
        /// the brand-new index as out of date again and throw
        /// <see cref="IndexOutOfDateException"/> on the first insert.
        /// </param>
        private static void CreateEmptyTableSkeleton(string srcPath, string destPath, int autoIncVal)
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
            ZeroRegion(header, ParadoxHeaderOffsets.ChangeCount1, 1);
            ZeroRegion(header, ParadoxHeaderOffsets.ChangeCount2, 1);
            ZeroRegion(header, ParadoxHeaderOffsets.MaxBlocks, 2);

            if (header.Length >= ParadoxHeaderOffsets.AutoIncVal + 4)
            {
                byte[] autoIncBytes = BitConverter.GetBytes(autoIncVal);
                Array.Copy(autoIncBytes, 0, header, ParadoxHeaderOffsets.AutoIncVal, 4);
            }

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
