using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using DbBlock = ParadoxReader.DataBlock; // Alias to avoid conflict with ParadoxFile.DataBlock

namespace ParadoxReader
{
    /// <summary>
    /// Extends ParadoxFile with full read/write support, including
    /// automatic maintenance of .PX and .Xnn index files, .MB blob file
    /// read/write support, and BDE-compatible file locking via .LCK files.
    /// This is the single class intended for both reading and writing
    /// Paradox tables (superseding the old read-only ParadoxTable).
    /// </summary>
    public class ParadoxTableFile : ParadoxFile
    {
        // ----------------------------------------------------------------
        // Constants
        // ----------------------------------------------------------------

        const string DOT_DB   = ".DB";  // Data file
        const string DOT_PX   = ".PX";  // Primary Key file
        const string DOT_MB   = ".MB";  // Blob file
        const string DOT_WILD = ".*";

        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly string          filePath; // Full path to the .DB file (not just the file name).
        private readonly BlockManager    blockManager;
        private readonly IndexManager    indexManager;
        private readonly ParadoxFileLock fileLock;
        private short                    tableVersion;

        public ParadoxPrimaryKey PrimaryKeyIndex;
        private ParadoxBlobFile  BlobFile;

        /// <summary>FilePath to this table's .DB file.</summary>
        public string FilePath => filePath;

        /// <summary>
        /// True if any index file (.PX/.Xnn/.Xgn/.Ynn/.Ygn) associated with
        /// this table was found to be out of date (its autoIncVal doesn't
        /// match this table's) when the table was opened. Checking this
        /// property never throws; attempting to actually read/write via an
        /// out-of-date index (e.g. <see cref="PrimaryKeyIndex"/>.Enumerate,
        /// a <see cref="SecondaryIndexHandle"/>.Enumerate, or any insert/
        /// update/delete that touches the index) throws
        /// <see cref="IndexOutOfDateException"/>. Consider
        /// <see cref="TableRebuilder"/> to regenerate stale indexes.
        /// </summary>
        public bool IndexOutOfDate => indexManager.IndexOutOfDate || (PrimaryKeyIndex?.IsOutOfDate ?? false);

        private List<SecondaryIndexHandle> secondaryIndexHandles;

        /// <summary>
        /// Read-side handles for every discovered secondary (.Xnn/.Xgn/.Ynn/.Ygn)
        /// index file, allowing condition-based index lookups analogous to
        /// <see cref="PrimaryKeyIndex"/>.
        /// </summary>
        public List<SecondaryIndexHandle> SecondaryIndexes
        {
            get
            {
                if (secondaryIndexHandles == null)
                {
                    secondaryIndexHandles = new List<SecondaryIndexHandle>();
                    foreach (var idx in indexManager.SecondaryIndexes)
                        secondaryIndexHandles.Add(new SecondaryIndexHandle(idx, this));
                }
                return secondaryIndexHandles;
            }
        }

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public ParadoxTableFile(string filePath) : base(filePath)
        {
            this.filePath = filePath;

            blockManager = new BlockManager(
                stream,
                headerSize,
                RecordSize,
                maxTableSize,
                EncryptionKey);

            indexManager = new IndexManager(
                filePath,
                FieldTypes,
                primaryKeyFields,
                autoIncVal);

            fileLock = new ParadoxFileLock(filePath);

            DiscoverAssociatedFiles(filePath);

            // changeCount4 lives at a fixed physical offset (0x70) even when the
            // file's version is too old for V4Header to be parsed (fileVersionID < 5).
            if (V4Header != null)
            {
                tableVersion = V4Header.changeCount4;
            }
            else
            {
                long savedPos = stream.Position;
                stream.Position = ParadoxHeaderOffsets.ChangeCount4;
                using (var r = new BinaryReader(new NonClosingStreamWrapper(stream), Encoding.Default))
                    tableVersion = r.ReadInt16();
                stream.Position = savedPos;
            }
        }

        /// <summary>
        /// Convenience constructor matching the old ParadoxTable(dbPath, tableName) signature.
        /// </summary>
        public ParadoxTableFile(string dbPath, string tableName)
            : this(Path.Combine(dbPath, tableName?.EnsureEndsWith(DOT_DB)))
        {
        }

        /// <summary>
        /// Locates and opens the associated .PX (primary key index) and
        /// .MB (blob/memo) files alongside the .DB file, if present.
        /// </summary>
        private void DiscoverAssociatedFiles(string dbFilePath)
        {
            var dbPath              = Path.GetDirectoryName(dbFilePath);
            var tableNameWithExt    = Path.GetFileName(dbFilePath);
            var tableNameWithoutExt = Path.GetFileNameWithoutExtension(tableNameWithExt);
            var files               = Directory.GetFiles(dbPath, tableNameWithoutExt + DOT_WILD);

            foreach (var file in files)
            {
                if (Path.GetFileName(file).Equals(tableNameWithExt, StringComparison.OrdinalIgnoreCase))
                    continue; // current file

                if (Path.GetFileNameWithoutExtension(file).EndsWith(DOT_PX, StringComparison.OrdinalIgnoreCase) ||
                    Path.GetExtension(file).Equals(DOT_PX, StringComparison.OrdinalIgnoreCase))
                {
                    this.PrimaryKeyIndex = new ParadoxPrimaryKey(this, file);
                    //break; // I'm not sure we can guarantee that PX will be found after MB.
                }
                if (Path.GetFileNameWithoutExtension(file).EndsWith(DOT_MB, StringComparison.OrdinalIgnoreCase) ||
                    Path.GetExtension(file).Equals(DOT_MB, StringComparison.OrdinalIgnoreCase))
                {
                    this.BlobFile = new ParadoxBlobFile(file);
                }
            }
        }

        // ----------------------------------------------------------------
        // Blob file support
        // ----------------------------------------------------------------

        internal override byte[] ReadBlob(byte[] blobInfo, int len, int hsize)
        {
            if (this.BlobFile == null)
            {
                return base.ReadBlob(blobInfo, len, hsize);
            }
            else
            {
                return this.BlobFile.ReadBlob(blobInfo, len, hsize);
            }
        }

        internal override void WriteBlob(byte[] blobInfo, int len, int hsize, byte[] blobVal)
        {
            if (this.BlobFile == null)
            {
                base.WriteBlob(blobInfo, len, hsize, blobVal);
            }
            else
            {
                this.BlobFile.WriteBlob(blobInfo, len, hsize, blobVal);
                EnsureHasBlobFlagSet();
            }
        }

        /// <summary>
        /// BDE sets a flag byte at .DB header offset 0x74 to 0x01 once the table has
        /// an associated (non-empty) blob/memo file. Files created before any memo
        /// value was ever written leave this byte 0x00; without it, BDE reports the
        /// memo/blob file as corrupt even though the data itself is well-formed.
        /// </summary>
        private void EnsureHasBlobFlagSet()
        {
            this.stream.Position = ParadoxHeaderOffsets.HasBlobFlag;
            int current = this.stream.ReadByte();
            if (current != 1)
            {
                this.stream.Position = ParadoxHeaderOffsets.HasBlobFlag;
                this.stream.WriteByte(1);
                this.stream.Flush();
            }
        }

        // ----------------------------------------------------------------
        // Insert
        // ----------------------------------------------------------------

        /// <summary>
        /// Inserts a new record and updates all index files.
        /// </summary>
        /// <param name="fieldValues">Values for all fields, in field order.</param>
        /// <returns>The newly inserted record including its block/record position.</returns>
        public ParadoxReader.ParadoxRecord InsertRecord(object[] fieldValues)
        {
            using (fileLock.AcquireScopedWriteLock())
            {
                byte[]   recordBytes = SerializeRecord(fieldValues);
                DbBlock  block       = FindOrAllocateBlockForInsert();
                ushort   recIdx      = (ushort)block.RecordCount;

                block.SetRecordBytes(recIdx, recordBytes);
                block.RecordCount++;
                blockManager.WriteBlock(block);

                RecordCount++;
                WriteRecordCountToHeader();

                NotifyBlockChanged(block);
                IncrementChangeCount();

                return new ParadoxRecord(block.BlockNumber, recIdx, fieldValues);
            }
        }

        // ----------------------------------------------------------------
        // Append
        // ----------------------------------------------------------------

        /// <summary>
        /// Appends a new record to the end of the table (always adds a new
        /// record rather than reusing/overwriting an existing slot), updates
        /// all index files, and manually assigns/advances any AutoInc field.
        /// </summary>
        /// <param name="fieldValues">Values for all fields, in field order. Any AutoInc field value is ignored and assigned automatically.</param>
        /// <returns>The newly appended record including its block/record position.</returns>
        public ParadoxReader.ParadoxRecord AppendRecord(object[] fieldValues)
        {
            using (fileLock.AcquireScopedWriteLock())
            {
                // Paradox manages AutoInc values itself (not via a normal index),
                // so we must assign/increment it manually before serializing.
                AssignAutoIncValues(fieldValues);

                byte[]   recordBytes = SerializeRecord(fieldValues);
                DbBlock  block       = FindOrAllocateBlockForInsert();
                ushort   recIdx      = (ushort)block.RecordCount;

                block.SetRecordBytes(recIdx, recordBytes);
                block.RecordCount++;
                blockManager.WriteBlock(block);

                RecordCount++;
                WriteRecordCountToHeader();

                NotifyBlockChanged(block);
                IncrementChangeCount();

                return new ParadoxRecord(block.BlockNumber, recIdx, fieldValues);
            }
        }

        /// <summary>
        /// Finds any AutoInc field(s)
        /// autoIncVal + 1) into fieldValues, and persists the new
        /// autoIncVal back to the file header.
        /// </summary>
        private void AssignAutoIncValues(object[] fieldValues)
        {
            bool assigned = false;

            for (int i = 0; i < FieldTypes.Length; i++)
            {
                if (FieldTypes[i].fType != ParadoxFieldTypes.AutoInc)
                    continue;

                autoIncVal++;
                fieldValues[i] = autoIncVal;
                assigned = true;
            }

            if (assigned)
            {
                WriteAutoIncValToHeader();
                indexManager.SyncAutoIncVal(autoIncVal);
            }
        }

        /// <summary>AutoIncVal (longint) is at offset 0x49.</summary>
        private void WriteAutoIncValToHeader()
        {
            stream.Position = ParadoxHeaderOffsets.AutoIncVal;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(stream), Encoding.Default))
                w.Write(autoIncVal);
        }

        // ----------------------------------------------------------------
        // Update
        // ----------------------------------------------------------------

        /// <summary>
        /// Updates an existing record in place and updates all index files.
        /// </summary>
        public void UpdateRecord(ParadoxReader.ParadoxRecord existing, object[] newFieldValues)
        {
            using (fileLock.AcquireScopedWriteLock())
            {
                byte[]  recordBytes = SerializeRecord(newFieldValues);
                DbBlock block       = blockManager.ReadBlock(existing.BlockNumber);

                block.SetRecordBytes(existing.RecordIndex, recordBytes);
                blockManager.WriteBlock(block);

                NotifyBlockChanged(block);
                IncrementChangeCount();

                // Refresh cached values on the record object
                existing.DataValues = newFieldValues;
            }
        }

        // ----------------------------------------------------------------
        // Delete
        // ----------------------------------------------------------------

        /// <summary>
        /// Deletes a record and updates all index files.
        /// </summary>
        public void DeleteRecord(ParadoxReader.ParadoxRecord record)
        {
            using (fileLock.AcquireScopedWriteLock())
            {
                DbBlock block = blockManager.ReadBlock(record.BlockNumber);

                // Shift all records after the deleted one down by one slot
                for (int i = record.RecordIndex; i < block.RecordCount - 1; i++)
                    block.SetRecordBytes(i, block.GetRecordBytes(i + 1));

                // Zero out the vacated last slot
                block.SetRecordBytes(block.RecordCount - 1, new byte[RecordSize]);
                block.RecordCount--;
                blockManager.WriteBlock(block);

                RecordCount--;
                WriteRecordCountToHeader();

                NotifyBlockChanged(block);
                IncrementChangeCount();
            }
        }

        // ----------------------------------------------------------------
        // Record serialization
        // ----------------------------------------------------------------

        private byte[] SerializeRecord(object[] fieldValues)
        {
            byte[] bytes = KeySerializer.Serialize(fieldValues, FieldTypes, this);
            if (bytes.Length != RecordSize)
                Array.Resize(ref bytes, RecordSize);
            return bytes;
        }

        /// <summary>
        /// Notifies the index manager that a .DB data block's contents have
        /// changed. Paradox leaf index entries are per-DB-BLOCK, not per-row
        /// (see PrimaryIndexFile/SecondaryIndexFile remarks), so index maintenance
        /// only needs the block's current first-row field values and record
        /// count — not the specific row that changed.
        /// </summary>
        private void NotifyBlockChanged(DbBlock block)
        {
            object[] firstRowValues = block.RecordCount > 0
                ? ParadoxRecord.DeserializeRecordBytes(block.GetRecordBytes(0), this)
                : null;

            indexManager.OnBlockChanged(firstRowValues, block.BlockNumber, block.RecordCount);
        }

        // ----------------------------------------------------------------
        // Block management
        // ----------------------------------------------------------------

        private DbBlock FindOrAllocateBlockForInsert()
        {
            // Empty table: no blocks exist yet
            if (fileBlocks == 0)
            {
                DbBlock first = blockManager.AllocateBlock();
                firstBlock = (ushort)(first.BlockNumber + 1); // header stores 1-based block numbers
                lastBlock  = (ushort)(first.BlockNumber + 1);
                nextBlock  = (ushort)(first.BlockNumber + 1);
                fileBlocks = 1;
                WriteBlockHeadersToFileHeader();
                return first;
            }

            // firstBlock/lastBlock in the header are 1-based; BlockManager works
            // with 0-based physical block numbers.
            ushort lastBlock0Based  = (ushort)(lastBlock  - 1);
            ushort firstBlock0Based = (ushort)(firstBlock - 1);

            // Try the last block first (most likely to have room)
            DbBlock candidate = blockManager.ReadBlock(lastBlock0Based);
            if (candidate.HasRoom) return candidate;

            // Paradox blocks only chain forward (NextBlock) — no back-pointer
            // exists, so walk forward from the first block looking for room.
            // Like firstBlock/lastBlock/nextBlock in the file header, a
            // block's own NextBlock field is 1-based (0 = none/end of chain),
            // NOT the raw 0-based BlockNumber, so it must be converted before
            // use as a physical block index.
            ushort current = firstBlock0Based;
            while (current != lastBlock0Based)
            {
                DbBlock block = blockManager.ReadBlock(current);
                if (block.HasRoom) return block;
                current = (ushort)(block.NextBlock - 1);
            }

            // All blocks are full — allocate a new last block
            DbBlock newBlock = blockManager.AllocateBlock();

            // Link old last → new block (1-based, matching the NextBlock
            // convention described above)
            DbBlock oldLast = blockManager.ReadBlock(lastBlock0Based);
            oldLast.NextBlock = (ushort)(newBlock.BlockNumber + 1);
            blockManager.WriteBlock(oldLast);

            // nextBlock must track the most-recently-allocated (i.e. now
            // last) block, same as lastBlock, or BDE sees a stale value and
            // considers the table corrupt/inconsistent. Confirmed against a
            // real BDE/SQLRunner-written table: after a block split,
            // nextBlock == lastBlock, whereas leaving nextBlock at its
            // original single-block value diverges from BDE's own output.
            lastBlock  = (ushort)(newBlock.BlockNumber + 1); // header stores 1-based
            nextBlock  = lastBlock;
            fileBlocks = (ushort)(fileBlocks + 1);
            WriteBlockHeadersToFileHeader();

            return newBlock;
        }

        // ----------------------------------------------------------------
        // Header updates
        // ----------------------------------------------------------------

        /// <summary>RecordCount is at offset 0x06 (int32), after RecordSize(2)+headerSize(2)+FileType(1)+maxTableSize(1).</summary>
        private void WriteRecordCountToHeader()
        {
            stream.Position = ParadoxHeaderOffsets.RecordCount;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(stream), Encoding.Default))
                w.Write(RecordCount);
        }

        /// <summary>
        /// changeCount4 (V4Hdr, offset 0x70, short) is the "table version"
        /// counter that BDE/Pdxrbld compares against each index's own copy of
        /// the value to decide whether the index is out of date. Must be
        /// incremented on every insert/update/delete and mirrored into every
        /// open index file. Written directly at the fixed physical offset
        /// regardless of whether V4Header was parsed (older fileVersionIDs
        /// don't parse it, but the header region and byte still exist).
        /// </summary>
        private void IncrementTableVersion()
        {
            short newValue = (short)((V4Header?.changeCount4 ?? tableVersion) + 1);
            tableVersion = newValue;
            if (V4Header != null) V4Header.changeCount4 = newValue;

            stream.Position = ParadoxHeaderOffsets.ChangeCount4;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(stream), Encoding.Default))
                w.Write(newValue);
        }

        /// <summary>
        /// Writes block chain fields back to the header.
        /// Layout (from ReadHeader): RecordSize(2)@0x00, headerSize(2)@0x02,
        /// FileType(1)@0x04, maxTableSize(1)@0x05, RecordCount(4)@0x06, then
        /// nextBlock(2) fileBlocks(2) firstBlock(2) lastBlock(2) starting at 0x0A.
        /// </summary>
        private void WriteBlockHeadersToFileHeader()
        {
            stream.Position = ParadoxHeaderOffsets.BlockChain;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(stream), Encoding.Default))
            {
                w.Write(nextBlock);
                w.Write(fileBlocks);
                w.Write(firstBlock);
                w.Write(lastBlock);
            }

            // maxBlocks (word @ 0x3A) tracks the allocated capacity of the block
            // chain. BDE/Pdxrbld flags the table as corrupt ("Total blocks greater
            // than max blocks") if fileBlocks ever exceeds it, so keep it in sync.
            if (fileBlocks > maxBlocks)
            {
                maxBlocks = fileBlocks;
                stream.Position = ParadoxHeaderOffsets.MaxBlocks;
                using (var w = new BinaryWriter(new NonClosingStreamWrapper(stream), Encoding.Default))
                    w.Write(maxBlocks);
            }
        }

        /// <summary>
        /// Increments changeCount1/changeCount2 so BDE detects that
        /// the table has been modified. Offsets 0x2D and 0x2E.
        /// Also mirrors the new value into every open index file so BDE
        /// doesn't consider them out of date relative to the .DB file.
        /// </summary>
        private void IncrementChangeCount()
        {
            changeCount1++;
            if (changeCount1 == 0) changeCount2++; // carry on overflow

            stream.Position = ParadoxHeaderOffsets.ChangeCount1;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(stream), Encoding.Default))
            {
                w.Write(changeCount1);
                w.Write(changeCount2);
            }

            indexManager.SyncChangeCount(changeCount1, changeCount2);

            IncrementTableVersion();

            indexManager.IncrementWriteCounter();
        }

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public override void Dispose()
        {
            fileLock?.Dispose();
            indexManager?.Dispose();
            PrimaryKeyIndex?.Dispose();
            BlobFile?.Dispose();
            base.Dispose();
        }
    }
}