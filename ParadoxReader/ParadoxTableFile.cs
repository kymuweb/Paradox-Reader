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
        private BlockManager    blockManager;
        private IndexManager    indexManager;
        private ParadoxFileLock fileLock;
        private short                    tableVersion;
        private bool                     indexesFromSuppliedStreams;

        public ParadoxPrimaryKey PrimaryKeyIndex;
        private ParadoxBlobFile  BlobFile;

        /// <summary>FilePath to this table's .DB file.</summary>
        public string FilePath => filePath;

        /// <summary>
        /// Holds the .LCK write lock for the duration of the returned scope,
        /// regardless of how many individual insert/update/delete calls
        /// happen within it. Normally each of those calls acquires and
        /// releases the lock on its own (writing/deleting the .LCK file
        /// every time), which is wasted disk I/O when many records are
        /// being written back-to-back in a single logical operation (e.g.
        /// <see cref="TableRebuilder"/> reinserting every record during a
        /// rebuild). Wrap such a loop in
        /// <c>using (table.AcquireScopedBatchWriteLock())</c> so the .LCK
        /// file is written once at the start and removed once at the end.
        /// </summary>
        internal ParadoxFileLock.ScopedWriteLock AcquireScopedBatchWriteLock() => fileLock.AcquireScopedWriteLock();

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
            InitializeAfterHeaderRead(filePath);
        }

        /// <summary>
        /// Opens a table whose .DB data is backed by an already-open
        /// <paramref name="dbStream"/> (e.g. a <see cref="MemoryStream"/>)
        /// rather than a file on disk, while still using
        /// <paramref name="filePath"/> to locate/manage associated files
        /// (.PX, .Xnn/.Xgn/.Ynn/.Ygn, .MB) and the .LCK lock file, and to
        /// report via <see cref="FilePath"/>. Used by
        /// <see cref="TableRebuilder"/>'s optional in-memory rebuild path
        /// to avoid per-record disk flushes while reinserting records;
        /// the caller is responsible for persisting the stream's final
        /// contents back to <paramref name="filePath"/> once finished.
        /// </summary>
        internal ParadoxTableFile(Stream dbStream, string filePath) : base(dbStream)
        {
            this.filePath = filePath;
            InitializeAfterHeaderRead(filePath);
        }

        /// <summary>
        /// Opens a table whose .DB data is backed by <paramref name="dbStream"/>,
        /// and whose associated index/blob files (.PX, .Xnn/.Xgn/.Ynn/.Ygn, .MB)
        /// are backed by the streams supplied in
        /// <paramref name="associatedFileStreams"/> (keyed by each file's full
        /// path), instead of being opened from disk. Any associated file
        /// present on disk but absent from <paramref name="associatedFileStreams"/>
        /// is still opened normally from disk. Used by
        /// <see cref="TableRebuilder"/>'s optional in-memory rebuild path so
        /// that no rebuild artifact touches disk until the caller explicitly
        /// persists each stream's final contents.
        /// </summary>
        internal ParadoxTableFile(Stream dbStream, string filePath, IDictionary<string, Stream> associatedFileStreams) : base(dbStream)
        {
            this.filePath = filePath;
            InitializeAfterHeaderRead(filePath, associatedFileStreams);
        }

        /// <summary>
        /// Convenience constructor matching the old ParadoxTable(dbPath, tableName) signature.
        /// </summary>
        public ParadoxTableFile(string dbPath, string tableName)
            : this(Path.Combine(dbPath, tableName?.EnsureEndsWith(DOT_DB)))
        {
        }

        /// <summary>
        /// Shared post-header-read initialization for both the disk-backed
        /// and stream-backed constructors: sets up block/index management,
        /// file locking, and associated-file discovery.
        /// </summary>
        private void InitializeAfterHeaderRead(string filePath, IDictionary<string, Stream> associatedFileStreams = null)
        {
            indexesFromSuppliedStreams = associatedFileStreams != null;
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
                autoIncVal,
                associatedFileStreams);

            fileLock = new ParadoxFileLock(filePath);

            DiscoverAssociatedFiles(filePath, associatedFileStreams);

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
        /// Locates and opens the associated .PX (primary key index) and
        /// .MB (blob/memo) files alongside the .DB file, if present.
        /// </summary>
        /// <summary>
        /// Locates and opens the associated .PX (primary key index) and
        /// .MB (blob/memo) files alongside the .DB file, if present. When
        /// <paramref name="associatedFileStreams"/> supplies a stream for a
        /// discovered file's full path, that stream is used in place of
        /// opening the file from disk.
        /// </summary>
        private void DiscoverAssociatedFiles(string dbFilePath, IDictionary<string, Stream> associatedFileStreams = null)
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
                    this.PrimaryKeyIndex = associatedFileStreams != null && associatedFileStreams.TryGetValue(file, out var pxStream)
                        ? new ParadoxPrimaryKey(this, pxStream, file)
                        : new ParadoxPrimaryKey(this, file);
                    //break; // I'm not sure we can guarantee that PX will be found after MB.
                }
                if (Path.GetFileNameWithoutExtension(file).EndsWith(DOT_MB, StringComparison.OrdinalIgnoreCase) ||
                    Path.GetExtension(file).Equals(DOT_MB, StringComparison.OrdinalIgnoreCase))
                {
                    this.BlobFile = associatedFileStreams != null && associatedFileStreams.TryGetValue(file, out var mbStream)
                        ? new ParadoxBlobFile(mbStream)
                        : new ParadoxBlobFile(file);
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
            ThrowIfReadOnly();
            using (fileLock.AcquireScopedWriteLock())
            {
                RefreshHeaderState();
                var headersBefore = CaptureFileHeaders();
                CheckUniqueIndexes(fieldValues);
                byte[]   recordBytes = SerializeRecord(fieldValues);
                DbBlock  block       = FindOrAllocateBlockForInsert();
                ushort   recIdx      = (ushort)block.RecordCount;

                block.SetRecordBytes(recIdx, recordBytes);
                block.RecordCount++;
                blockManager.WriteBlock(block);

                RecordCount++;
                WriteRecordCountToHeader();

                NotifyBlockChanged(block);
                indexManager.OnRowAppended(fieldValues, block.BlockNumber);
                indexManager.SyncRecordCount(RecordCount);
                IncrementChangeCount();
                VerifyFileHeadersChanged(headersBefore, "InsertRecord");

                return new ParadoxRecord(block.BlockNumber, recIdx, fieldValues);
            }
        }

        /// <summary>
        /// Throws
        /// key in any unique
        /// </summary>
        private void CheckUniqueIndexes(object[] fieldValues)
        {
            if (primaryKeyFields > 0)
            {
                // Primary keys are always unique; Alpha comparison is case-sensitive (unverified against BDE).
                var pk = new int[primaryKeyFields];
                for (int i = 0; i < pk.Length; i++) pk[i] = i;
                CheckUniqueKey(filePath, pk, true, fieldValues);
            }

            foreach (var idx in indexManager.SecondaryIndexes)
            {
                if (!idx.IsUnique) continue;

                // Leading IndexedFieldCount fields are the user's indexed fields; the rest are appended primary-key fields.
                var fi = idx.FieldIndices;
                int n = Math.Min(idx.IndexedFieldCount, fi.Length);
                var used = new int[n];
                Array.Copy(fi, used, n);
                CheckUniqueKey(idx.FilePath, used, idx.IsCaseSensitive, fieldValues);
            }
        }

        private void CheckUniqueKey(string keyFilePath, int[] fi, bool caseSensitive, object[] fieldValues)
        {
            {
                int n = fi.Length;
                bool skip = false;
                var keyVals = new object[n];
                for (int i = 0; i < n; i++)
                {
                    keyVals[i] = fi[i] < fieldValues.Length ? fieldValues[fi[i]] : null;
                    if (keyVals[i] == null || keyVals[i] is DBNull || FieldTypes[fi[i]].fType == ParadoxFieldTypes.AutoInc) skip = true;
                }
                if (skip) return;

                var keyStrings = new string[n];
                for (int i = 0; i < n; i++)
                    keyStrings[i] = KeyToString(keyVals[i], caseSensitive);

                foreach (var rec in Enumerate())
                {
                    bool same = true;
                    for (int i = 0; i < n && same; i++)
                    {
                        var existing = rec.DataValues[fi[i]];
                        if (existing == null || existing is DBNull ||
                            KeyToString(existing, caseSensitive) != keyStrings[i])
                            same = false;
                    }
                    if (same)
                        throw new KeyViolationException(keyFilePath,
                            $"Key violation: unique index '{System.IO.Path.GetFileName(keyFilePath)}' already contains key '{string.Join(", ", keyStrings)}'.");
                }
            }
        }

        private static string KeyToString(object value, bool caseSensitive)
        {
            string s = value is string str ? str.TrimEnd('\0', ' ') : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
            return caseSensitive ? s : s.ToUpperInvariant();
        }

        // ----------------------------------------------------------------
        // Append
        // ----------------------------------------------------------------

        /// <summary>
        /// Opens the table, appends one record, and closes it again. Preferred over keeping a
        /// long-lived <see cref="ParadoxTableFile"/> open when other processes/machines may also
        /// write to the same table: nothing is cached between writes.
        /// </summary>
        public static ParadoxReader.ParadoxRecord AppendRecord(string dbFilePath, object[] fieldValues)
        {
            using (var table = new ParadoxTableFile(dbFilePath))
                return table.AppendRecord(fieldValues);
        }

        /// <summary>
        /// Appends a new record to the end of the table (always adds a new
        /// record rather than reusing/overwriting an existing slot), updates
        /// all index files, and manually assigns/advances any AutoInc field.
        /// </summary>
        /// <param name="fieldValues">Values for all fields, in field order. Any AutoInc field value is ignored and assigned automatically.</param>
        /// <returns>The newly appended record including its block/record position.</returns>
        public ParadoxReader.ParadoxRecord AppendRecord(object[] fieldValues)
        {
            ThrowIfReadOnly();
            using (fileLock.AcquireScopedWriteLock())
            {
                int recordCountBefore = RecordCount;
                try
                {
                    return AppendRecordLocked(fieldValues);
                }
                catch (Exception ex)
                {
                    ReportWriteException("AppendRecord", ex, recordCountBefore);
                    throw;
                }
            }
        }

        /// <summary>
        /// Reports an exception thrown part-way through a write. Such a write may already have added the
        /// row to the .DB without updating the indexes, and post-write verification never runs for it.
        /// </summary>
        private void ReportWriteException(string operation, Exception ex, int recordCountBefore)
        {
            if (!VerifyWrites) return;
            string msg = $"ParadoxReader {operation} on '{filePath}' threw {ex.GetType().Name}: {ex.Message} " +
                         $"(RecordCount before={recordCountBefore}, after={RecordCount}, AutoInc={autoIncVal}) {ex.StackTrace}";
            System.Diagnostics.Trace.TraceError(msg);
            try { WriteVerificationFailed?.Invoke(filePath, operation, msg); } catch { /* never let a handler mask the original exception */ }
        }

        /// <summary>
        /// Appends many records under a single write lock. The header state is refreshed once and
        /// <see cref="VerifyWrites"/> runs once at the end instead of after every row, which is much
        /// faster than calling <see cref="AppendRecord(object[])"/> in a loop or reopening the table per row.
        /// Rows are written in order; if one throws, earlier rows remain written and the exception is rethrown.
        /// </summary>
        /// <param name="rows">Field values for each record, in field order. AutoInc values are assigned automatically.</param>
        /// <returns>The appended records, in input order.</returns>
        public IList<ParadoxReader.ParadoxRecord> AppendRecords(IEnumerable<object[]> rows)
        {
            if (rows == null) throw new ArgumentNullException(nameof(rows));
            ThrowIfReadOnly();
            var result = new List<ParadoxReader.ParadoxRecord>();
            using (fileLock.AcquireScopedWriteLock())
            {
                int recordCountBefore = RecordCount;
                Dictionary<string, byte[]> headersBefore = null;
                try
                {
                    RefreshHeaderState();
                    headersBefore = CaptureFileHeaders();
                    foreach (var fieldValues in rows)
                        result.Add(AppendRecordCore(fieldValues));
                }
                catch (Exception ex)
                {
                    ReportWriteException($"AppendRecords (row {result.Count + 1})", ex, recordCountBefore);
                    throw;
                }
                if (result.Count > 0) VerifyFileHeadersChanged(headersBefore, $"AppendRecords ({result.Count} rows)");
            }
            return result;
        }

        private ParadoxReader.ParadoxRecord AppendRecordLocked(object[] fieldValues)
        {
            RefreshHeaderState();
            var headersBefore = CaptureFileHeaders();
            var record = AppendRecordCore(fieldValues);
            VerifyFileHeadersChanged(headersBefore, "AppendRecord");
            return record;
        }

        /// <summary>Appends one row. Caller holds the write lock and has refreshed header state.</summary>
        private ParadoxReader.ParadoxRecord AppendRecordCore(object[] fieldValues)
        {
            if (fieldValues == null) throw new ArgumentNullException(nameof(fieldValues));
            {
                CheckUniqueIndexes(fieldValues);

                // Paradox manages
                // so we must assign/increment it manually before serializing.
                AssignAutoIncValues(fieldValues);

                byte[]   recordBytes = SerializeRecord(fieldValues);

                if (RequiresSortedInsert())
                {
                    return AppendSorted(fieldValues, recordBytes);
                }

                DbBlock  block       = FindOrAllocateBlockForInsert();
                ushort   recIdx      = (ushort)block.RecordCount;

                block.SetRecordBytes(recIdx, recordBytes);
                block.RecordCount++;
                blockManager.WriteBlock(block);

                RecordCount++;
                WriteRecordCountToHeader();

                NotifyBlockChanged(block);
                indexManager.OnRowAppended(fieldValues, block.BlockNumber);
                indexManager.SyncRecordCount(RecordCount);
                IncrementChangeCount();

                return new ParadoxRecord(block.BlockNumber, recIdx, fieldValues);
            }
        }

        // ----------------------------------------------------------------
        // Post-write verification
        // ----------------------------------------------------------------

        /// <summary>
        /// Optional; off by default. When true, every write on this instance captures the headers of the .DB, .PX, .Xnn/.Xgn and .Ynn/.Ygn files
        /// before the write and checks afterwards that each one changed (every write advances a
        /// change/version counter in all of them). Files left untouched indicate an index that was
        /// not updated. Failures are reported via <see cref="WriteVerificationFailed"/> and
        /// <see cref="System.Diagnostics.Trace"/>; the write itself is not rolled back.
        /// </summary>
        public bool VerifyWrites { get; set; }

        /// <summary>Raised when <see cref="VerifyWrites"/> finds a table/index file that a write did not modify.
        /// Arguments: .DB path, operation name, message listing the unmodified files.</summary>
        public event Action<string, string, string> WriteVerificationFailed;

        private const int VerifyHeaderBytes = 0x80;

        private Dictionary<string, byte[]> CaptureFileHeaders()
        {
            if (!VerifyWrites) return null;
            var result = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
            string dir  = Path.GetDirectoryName(filePath);
            string name = Path.GetFileNameWithoutExtension(filePath);
            foreach (var f in Directory.GetFiles(string.IsNullOrEmpty(dir) ? "." : dir, name + DOT_WILD))
            {
                if (!string.Equals(Path.GetFileNameWithoutExtension(f), name, StringComparison.OrdinalIgnoreCase)) continue;
                string ext = Path.GetExtension(f).ToUpperInvariant();
                bool tracked = ext == DOT_DB || ext == DOT_PX ||
                               (ext.Length == 4 && (ext[1] == 'X' || ext[1] == 'Y') && Uri.IsHexDigit(ext[3]));
                if (tracked) result[f] = ReadHeaderBytes(f);
            }
            return result;
        }

        private static byte[] ReadHeaderBytes(string path)
        {
            var buf = new byte[VerifyHeaderBytes];
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 1))
            {
                int read = 0, n;
                while (read < buf.Length && (n = fs.Read(buf, read, buf.Length - read)) > 0) read += n;
            }
            return buf;
        }

        /// <summary>
        /// Walks a flat index file's block chain (block headers only) and counts the actual entries.
        /// The XGn header record count is re-synced on every write, so it cannot reveal missing entries.
        /// </summary>
        private static string CountChainRows(string path, out int rows, out int blocks)
        {
            rows = 0; blocks = 0;
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 1))
            using (var r = new BinaryReader(fs))
            {
                ushort recSize    = r.ReadUInt16();
                ushort headerSize = r.ReadUInt16();
                fs.Position = 5;
                int blockSize = fs.ReadByte() * 0x400;
                fs.Position = 0x0C;
                ushort fileBlocks = r.ReadUInt16();
                ushort block      = r.ReadUInt16();
                if (recSize == 0 || blockSize <= 0) return "invalid header";
                var seen = new HashSet<ushort>();
                ushort prev = 0;
                while (block != 0)
                {
                    if (block > fileBlocks || !seen.Add(block)) return $"broken block chain at block {block}";
                    fs.Position = headerSize + (long)(block - 1) * blockSize;
                    ushort next     = r.ReadUInt16();
                    ushort prevLink = r.ReadUInt16();
                    short  used     = r.ReadInt16();
                    if (prevLink != prev) return $"block {block} prev link {prevLink} != {prev}";
                    if (used >= 0) rows += used / recSize + 1;
                    blocks++;
                    prev  = block;
                    block = next;
                }
            }
            return null;
        }

        private void VerifyFileHeadersChanged(Dictionary<string, byte[]> before, string operation)
        {
            if (before == null) return;
            var unchanged = new List<string>();
            var problems  = new List<string>();
            var after     = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in before)
            {
                byte[] now;
                try { now = ReadHeaderBytes(kv.Key); }
                catch (Exception ex) { problems.Add(Path.GetFileName(kv.Key) + " unreadable: " + ex.Message); continue; }
                after[kv.Key] = now;
                bool same = true;
                for (int i = 0; i < now.Length && same; i++) same = now[i] == kv.Value[i];
                if (same) unchanged.Add(Path.GetFileName(kv.Key));
            }

            // Row counts: every XGn holds one row per table record; every YGn holds one row per
            // block of its XGn (verified against BDE/Pdxrbld output).
            int dbCount = RecordCount;
            foreach (var kv in after)
            {
                string ext = Path.GetExtension(kv.Key).ToUpperInvariant();
                if (ext.Length != 4 || ext[2] != 'G') continue;
                int rc = BitConverter.ToInt32(kv.Value, ParadoxHeaderOffsets.RecordCount);
                if (ext[1] == 'X')
                {
                    if (rc != dbCount) problems.Add($"{Path.GetFileName(kv.Key)} header rows {rc} != DB rows {dbCount}");
                    int rows, blocks;
                    string chainErr = CountChainRows(kv.Key, out rows, out blocks);
                    if (chainErr != null) problems.Add($"{Path.GetFileName(kv.Key)} {chainErr}");
                    else if (rows != dbCount) problems.Add($"{Path.GetFileName(kv.Key)} actual entries {rows} != DB rows {dbCount}");
                }
                else if (ext[1] == 'Y')
                {
                    string xg = Path.ChangeExtension(kv.Key, ".XG" + ext[3]);
                    byte[] xh = null;
                    foreach (var o in after) if (string.Equals(o.Key, xg, StringComparison.OrdinalIgnoreCase)) xh = o.Value;
                    if (xh == null) continue;
                    int xgBlocks = BitConverter.ToUInt16(xh, 0x0C);
                    if (rc != xgBlocks) problems.Add($"{Path.GetFileName(kv.Key)} rows {rc} != {Path.GetFileName(xg)} blocks {xgBlocks}");
                }
            }

            if (unchanged.Count > 0) problems.Insert(0, "did not modify: " + string.Join(", ", unchanged));
            if (problems.Count == 0) return;

            string msg = $"ParadoxReader {operation} on '{filePath}': {string.Join("; ", problems)} (RecordCount={RecordCount}, AutoInc={autoIncVal})";
            System.Diagnostics.Trace.TraceError(msg);
            try { WriteVerificationFailed?.Invoke(filePath, operation, msg); } catch { /* never let a handler break the write */ }
        }

        /// <summary>
        /// Paradox keeps
        /// AutoInc keys are monotonically increasing, so plain appending is already ordered.
        /// </summary>
        private bool RequiresSortedInsert()
        {
            if (primaryKeyFields <= 0 || fileBlocks == 0 || indexManager.PrimaryIndexFile == null)
                return false;
            for (int i = 0; i < primaryKeyFields; i++)
                if (FieldTypes[i].fType != ParadoxFieldTypes.AutoInc)
                    return true;
            return false;
        }

        private byte[] KeyOfValues(object[] values)
        {
            var keyFields = new ParadoxFile.FieldInfo[primaryKeyFields];
            var keyValues = new object[primaryKeyFields];
            for (int i = 0; i < primaryKeyFields; i++)
            {
                keyFields[i] = FieldTypes[i];
                keyValues[i] = values[i];
            }
            return KeySerializer.Serialize(keyValues, keyFields);
        }

        private int CompareKeyOf(object[] a, object[] b)
        {
            var keyFields = new ParadoxFile.FieldInfo[primaryKeyFields];
            for (int i = 0; i < primaryKeyFields; i++) keyFields[i] = FieldTypes[i];
            return KeySerializer.Compare(KeyOfValues(a), KeyOfValues(b), keyFields);
        }

        private ParadoxReader.ParadoxRecord AppendSorted(object[] fieldValues, byte[] recordBytes)
        {
            // Find the last block (in chain order) whose first key <= new key; default first block.
            DbBlock target = blockManager.ReadBlock((ushort)(firstBlock - 1));
            DbBlock cur    = target;
            while (true)
            {
                object[] first = ParadoxRecord.DeserializeRecordBytes(cur.GetRecordBytes(0), this);
                if (CompareKeyOf(first, fieldValues) <= 0) target = cur;
                else break;
                if (cur.NextBlock == 0) break;
                cur = blockManager.ReadBlock((ushort)(cur.NextBlock - 1));
            }

            // Collect existing records + the new one in key order.
            int count = target.RecordCount;
            var recs = new System.Collections.Generic.List<byte[]>();
            int pos = -1;
            for (int i = 0; i < count; i++)
            {
                byte[] rb = target.GetRecordBytes(i);
                if (pos < 0)
                {
                    int c = CompareKeyOf(fieldValues, ParadoxRecord.DeserializeRecordBytes(rb, this));
                    if (c == 0)
                        throw new InvalidOperationException("Key violation: a record with the same primary key already exists.");
                    if (c < 0) { pos = i; recs.Add(recordBytes); }
                }
                recs.Add(rb);
            }
            if (pos < 0) { pos = recs.Count; recs.Add(recordBytes); }

            DbBlock second = null;
            int keep = recs.Count;
            if (recs.Count > target.MaxRecords)
            {
                second = blockManager.AllocateBlock();
                keep = (recs.Count + 1) / 2;

                second.NextBlock = target.NextBlock;
                second.PrevBlock = (ushort)(target.BlockNumber + 1);
                if (target.NextBlock != 0)
                {
                    DbBlock after = blockManager.ReadBlock((ushort)(target.NextBlock - 1));
                    after.PrevBlock = (ushort)(second.BlockNumber + 1);
                    blockManager.WriteBlock(after);
                }
                target.NextBlock = (ushort)(second.BlockNumber + 1);
                if (lastBlock == (ushort)(target.BlockNumber + 1))
                    lastBlock = (ushort)(second.BlockNumber + 1);
                fileBlocks = (ushort)(fileBlocks + 1);
                // BDE keeps header nextBlock == number of allocated blocks (not the chain's last block).
                nextBlock = fileBlocks;
                WriteBlockHeadersToFileHeader();
            }

            for (int i = 0; i < target.MaxRecords; i++)
                target.SetRecordBytes(i, i < keep ? recs[i] : new byte[RecordSize]);
            target.RecordCount = keep;
            blockManager.WriteBlock(target);

            ushort recBlock = target.BlockNumber;
            ushort recIdx   = (ushort)pos;

            if (second != null)
            {
                int n2 = recs.Count - keep;
                for (int i = 0; i < second.MaxRecords; i++)
                    second.SetRecordBytes(i, i < n2 ? recs[keep + i] : new byte[RecordSize]);
                second.RecordCount = n2;
                blockManager.WriteBlock(second);
                if (pos >= keep) { recBlock = second.BlockNumber; recIdx = (ushort)(pos - keep); }
            }

            RecordCount++;
            WriteRecordCountToHeader();

            NotifyBlockChanged(target);
            if (second != null) NotifyBlockChanged(second);
            indexManager.OnRowAppended(fieldValues, recBlock);
            indexManager.SyncRecordCount(RecordCount);
            IncrementChangeCount();

            return new ParadoxRecord(recBlock, recIdx, fieldValues);
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

                // Another writer may have advanced the on-disk counter since this instance was opened.
                stream.Position = ParadoxHeaderOffsets.AutoIncVal;
                int diskVal;
                using (var r = new BinaryReader(new NonClosingStreamWrapper(stream), Encoding.Default))
                    diskVal = r.ReadInt32();
                if (diskVal > autoIncVal)
                    autoIncVal = diskVal;

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
        /// <summary>
        /// Re-reads the mutable header fields from disk. Must be called right after the write lock is
        /// acquired: other writers (other processes/machines) may have changed them since this
        /// instance was opened, and writing back stale values duplicates AutoInc IDs and loses counts.
        /// </summary>
        private void RefreshHeaderState()
        {
            int    oldCount   = RecordCount;
            int    oldAutoInc = autoIncVal;
            short  oldVersion = tableVersion;
            ushort oldBlocks  = fileBlocks;
            ushort oldLast    = lastBlock;

            long saved = stream.Position;
            using (var r = new BinaryReader(new NonClosingStreamWrapper(stream), Encoding.Default))
            {
                stream.Position = ParadoxHeaderOffsets.RecordCount;
                RecordCount = r.ReadInt32();
                stream.Position = ParadoxHeaderOffsets.BlockChain;
                nextBlock  = r.ReadUInt16();
                fileBlocks = r.ReadUInt16();
                firstBlock = r.ReadUInt16();
                lastBlock  = r.ReadUInt16();
                stream.Position = ParadoxHeaderOffsets.ChangeCount1;
                changeCount1 = r.ReadByte();
                changeCount2 = r.ReadByte();
                stream.Position = ParadoxHeaderOffsets.AutoIncVal;
                int disk = r.ReadInt32();
                if (disk > autoIncVal) autoIncVal = disk;
                stream.Position = ParadoxHeaderOffsets.ChangeCount4;
                short ver = r.ReadInt16();
                tableVersion = ver;
                if (V4Header != null) V4Header.changeCount4 = ver;
            }
            stream.Position = saved;

            bool changedExternally = RecordCount != oldCount || autoIncVal != oldAutoInc ||
                                     tableVersion != oldVersion || fileBlocks != oldBlocks || lastBlock != oldLast;
            if (changedExternally && !indexesFromSuppliedStreams)
                ReloadIndexes();
        }

        /// <summary>
        /// Discards all in-memory index state (PX/XGn/YGn block maps, record counts, flat-index caches)
        /// and reopens the index files from disk, so writes after another writer's changes start from
        /// the current on-disk indexes instead of this instance's stale view.
        /// </summary>
		/// <summary>True when opened via <see cref="OpenReadOnly"/>; all files are opened read-only and writes throw.</summary>
		public bool IsReadOnly { get; private set; }

		/// <summary>
		/// Opens the table and all of its index/blob files with read-only access, for readers such as
		/// caches that must never write. Any write method throws <see cref="InvalidOperationException"/>.
		/// </summary>
		public static ParadoxTableFile OpenReadOnly(string filePath)
		{
			bool prev = ParadoxFileOpen.ReadOnlyScope;
			ParadoxFileOpen.ReadOnlyScope = true;
			try
			{
				var t = new ParadoxTableFile(filePath);
				t.IsReadOnly = true;
				return t;
			}
			finally { ParadoxFileOpen.ReadOnlyScope = prev; }
		}

		private void ThrowIfReadOnly()
		{
			if (IsReadOnly) throw new InvalidOperationException($"'{filePath}' was opened read-only.");
		}

		private void EnsureWriteLockHeld()
		{
			if (!fileLock.IsHeld)
				throw new InvalidOperationException($"Write to '{filePath}' attempted without holding the table write lock.");
		}
        private void ReloadIndexes()
        {
            indexManager?.Dispose();
            PrimaryKeyIndex?.Dispose();
            BlobFile?.Dispose();
            PrimaryKeyIndex = null;
            BlobFile = null;
            secondaryIndexHandles = null;

            bool prevScope = ParadoxFileOpen.ReadOnlyScope;
            ParadoxFileOpen.ReadOnlyScope = IsReadOnly;
            try
            {
                indexManager = new IndexManager(filePath, FieldTypes, primaryKeyFields, autoIncVal, null);
                DiscoverAssociatedFiles(filePath);
            }
            finally { ParadoxFileOpen.ReadOnlyScope = prevScope; }
        }

        private void WriteAutoIncValToHeader()
        {
            EnsureWriteLockHeld();
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
            ThrowIfReadOnly();
            using (fileLock.AcquireScopedWriteLock())
            {
                RefreshHeaderState();
                var headersBefore = CaptureFileHeaders();
                byte[]  recordBytes = SerializeRecord(newFieldValues);
                DbBlock block       = blockManager.ReadBlock(existing.BlockNumber);

                block.SetRecordBytes(existing.RecordIndex, recordBytes);
                blockManager.WriteBlock(block);

                NotifyBlockChanged(block);
                IncrementChangeCount();
                VerifyFileHeadersChanged(headersBefore, "UpdateRecord");

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
            ThrowIfReadOnly();
            using (fileLock.AcquireScopedWriteLock())
            {
                RefreshHeaderState();
                var headersBefore = CaptureFileHeaders();
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
                VerifyFileHeadersChanged(headersBefore, "DeleteRecord");
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
            EnsureWriteLockHeld();
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

            // Keyed tables must stay in primary-key order: an appended (highest) key can only go
            // into the last block. BDE leaves free slots in earlier blocks, so filling those would
            // put records out of sort order and desync the .PX/.XGn indexes.
            ushort current = primaryKeyFields > 0 ? lastBlock0Based : firstBlock0Based;

            // Paradox blocks only chain forward (NextBlock) — no back-pointer
            // exists, so walk forward from the first block looking for room.
            // Like firstBlock/lastBlock/nextBlock in the file header, a
            // block's own NextBlock field is 1-based (0 = none/end of chain),
            // NOT the raw 0-based BlockNumber, so it must be converted before
            // use as a physical block index.
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
            newBlock.PrevBlock = lastBlock;
            blockManager.WriteBlock(newBlock);

            // nextBlock must track the most-recently-allocated (i.e. now
            // last) block, same as lastBlock, or BDE sees a stale value and
            // considers the table corrupt/inconsistent. Confirmed against a
            // real BDE/SQLRunner-written table: after a block split,
            // nextBlock == lastBlock, whereas leaving nextBlock at its
            // original single-block value diverges from BDE's own output.
            lastBlock  = (ushort)(newBlock.BlockNumber + 1); // header stores 1-based
            fileBlocks = (ushort)(fileBlocks + 1);
            nextBlock  = fileBlocks;
            WriteBlockHeadersToFileHeader();

            return newBlock;
        }

        // ----------------------------------------------------------------
        // Header updates
        // ----------------------------------------------------------------

        /// <summary>RecordCount is at offset 0x06 (int32), after RecordSize(2)+headerSize(2)+FileType(1)+maxTableSize(1).</summary>
        private void WriteRecordCountToHeader()
        {
            EnsureWriteLockHeld();
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
        private short IncrementTableVersion()
        {
            EnsureWriteLockHeld();
            short newValue = (short)((V4Header?.changeCount4 ?? tableVersion) + 1);
            tableVersion = newValue;
            if (V4Header != null) V4Header.changeCount4 = newValue;

            stream.Position = ParadoxHeaderOffsets.ChangeCount4;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(stream), Encoding.Default))
                w.Write(newValue);

            return newValue;
        }

        /// <summary>
        /// Writes block chain fields back to the header.
        /// Layout (from ReadHeader): RecordSize(2)@0x00, headerSize(2)@0x02,
        /// FileType(1)@0x04, maxTableSize(1)@0x05, RecordCount(4)@0x06, then
        /// nextBlock(2) fileBlocks(2) firstBlock(2) lastBlock(2) starting at 0x0A.
        /// </summary>
        private void WriteBlockHeadersToFileHeader()
        {
            EnsureWriteLockHeld();
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
            EnsureWriteLockHeld();
            changeCount1++;
            if (changeCount1 == 0)
            {
                // Legacy wraps modulo 255 (0xFF -> 0x01) and does NOT carry into changeCount2:
                // BDE compares DB 0x2E with each XGn's 0x2E, so bumping it reports
                // "Index version does not match table version" (verified: 710 legacy writes 0x2E stays 0x84).
                changeCount1 = 1;
            }

            stream.Position = ParadoxHeaderOffsets.ChangeCount1;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(stream), Encoding.Default))
            {
                w.Write(changeCount1);
                w.Write(changeCount2);
            }

            indexManager.SyncChangeCount(changeCount1, changeCount2);

            short newTableVersion = IncrementTableVersion();
            indexManager.SyncTableVersion(newTableVersion);

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