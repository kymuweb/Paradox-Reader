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

            var fieldTypes = table.FieldTypes;

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
            //    under the temp base name: same schema/header, but with
            //    RecordCount/block-chain/index-tree/change-counter fields
            //    reset to "empty".
            // ------------------------------------------------------------
            var swapPairs = new List<FilePair>();
            foreach (var src in sourceFiles)
            {
                string ext  = Path.GetExtension(src);
                string dest = Path.Combine(dir, tempBaseName + ext);

                if (ext.Equals(".MB", StringComparison.OrdinalIgnoreCase))
                    CreateEmptyBlobSkeleton(src, dest);
                else
                    CreateEmptyTableSkeleton(src, dest);

                swapPairs.Add(new FilePair(src, dest));
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
                foreach (var original in records)
                {
                    var values = (object[])original.Clone();
                    ClearStaleBlobReferences(values, fieldTypes);
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
        private static void CreateEmptyTableSkeleton(string srcPath, string destPath)
        {
            byte[] header = ReadHeaderBytes(srcPath);

            ZeroRegion(header, ParadoxHeaderOffsets.RecordCount, 4);
            ZeroRegion(header, ParadoxHeaderOffsets.BlockChain, 8); // nextBlock+fileBlocks+firstBlock+lastBlock
            ZeroRegion(header, ParadoxHeaderOffsets.PxRootBlockId, 2);
            ZeroRegion(header, ParadoxHeaderOffsets.PxLevelCount, 1);
            ZeroRegion(header, ParadoxHeaderOffsets.ChangeCount1, 1);
            ZeroRegion(header, ParadoxHeaderOffsets.ChangeCount2, 1);
            ZeroRegion(header, ParadoxHeaderOffsets.MaxBlocks, 2);

            // changeCount4 (V4Hdr) only physically exists when the header
            // region is large enough to contain it.
            if (header.Length >= ParadoxHeaderOffsets.ChangeCount4 + 2)
                ZeroRegion(header, ParadoxHeaderOffsets.ChangeCount4, 2);

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
