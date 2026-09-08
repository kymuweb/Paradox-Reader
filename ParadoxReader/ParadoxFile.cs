using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    /// <remarks>
    /// FIELD OFFSETS AND KNOWN VALUES (base header, before the optional V4
    /// header extension - see <see cref="V4Hdr"/> for offsets 0x58-0x77).
    /// Offsets below are absolute file offsets, in the order read by
    /// <see cref="ReadHeader"/>; each field's summary gives its offset,
    /// wire type/size, and - where empirically confirmed against real
    /// BDE/SQLRunner-created fixtures (see ParadoxTest\data,
    /// ParadoxTest.HeaderCompareTest, and ParadoxTest.BdeConfigCompareTest) -
    /// the value(s) a from-scratch writer needs to produce. See also
    /// <see cref="ParadoxHeaderOffsets"/> (the subset of these offsets
    /// exposed as named constants for direct-seek callers) and
    /// <see cref="ParadoxHeaderBuilder"/> (the authoritative from-scratch
    /// writer implementing all of the below).
    /// </remarks>
    public partial class ParadoxFile : IDisposable
    {
        public string TableName;

        /// <summary>RecordSize (uint16) @ 0x00. Row data length in bytes (sum of field sizes); for .PX files, keyDataSize + 6 (see <see cref="ParadoxHeaderBuilder.BuildPxHeader"/>).</summary>
        public ushort RecordSize { get; private set; }

        /// <summary>headerSize (uint16) @ 0x02. Always padded to a full block (2048 bytes) for a freshly created empty table/index file, regardless of block size (see <see cref="ParadoxHeaderBuilder"/>'s DefaultHeaderSize remarks) - NOT simply maxTableSize * 0x400.</summary>
        internal ushort headerSize;

        /// <summary>FileType (byte) @ 0x04. See <see cref="ParadoxFileType"/> (DbFileIndexed/DbFileNotIndexed/PxFile/XgnFile.../YgnFile/etc).</summary>
        public ParadoxFileType FileType { get; private set; }

        /// <summary>
        /// maxTableSize (byte) @ 0x05. Block size in KB (blockSize = maxTableSize * 1024).
        /// For .DB/.XGn/.Xnn/.Ynn files this follows the BDE's configured
        /// PARADOX/idapi32.cfg BLOCK SIZE setting (this library defaults new
        /// tables to 32, i.e. 32768-byte blocks/~4GB max table size - see
        /// <see cref="ParadoxHeaderBuilder"/>'s DefaultMaxTableSize). For .PX
        /// and .YGn files, real BDE always uses a FIXED value of 2 (2048-byte
        /// blocks) regardless of the table's configured block size - confirmed
        /// via ParadoxTest.BdeConfigCompareTest (see FixedSmallMaxTableSize).
        /// </summary>
        internal byte maxTableSize;

        public int RecordCount { get; internal set; }

        /// <summary>nextBlock (uint16) @ 0x0A. Part of the block-chain pointers (see <see cref="ParadoxHeaderOffsets.BlockChain"/>); 0 for a freshly created empty file.</summary>
        internal ushort nextBlock;

        /// <summary>fileBlocks (uint16) @ 0x0C. Total data blocks currently allocated; 0 for a freshly created empty file.</summary>
        internal ushort fileBlocks;

        /// <summary>firstBlock (uint16) @ 0x0E. 0 for a freshly created empty file.</summary>
        internal ushort firstBlock;

        /// <summary>lastBlock (uint16) @ 0x10. 0 for a freshly created empty file.</summary>
        internal ushort lastBlock;

        /// <summary>unknown12x13 (uint16) @ 0x12. Constant 0x0006 across every real BDE-created fixture (.DB and .PX alike, regardless of schema) - confirmed empirically.</summary>
        internal ushort unknown12x13;

        /// <summary>modifiedFlags1 (byte) @ 0x14. 0 for a freshly created file.</summary>
        internal byte modifiedFlags1;

        /// <summary>indexFieldNumber (byte) @ 0x15. Only meaningful on secondary index files (1-based index of the indexed field); 0 elsewhere.</summary>
        internal byte indexFieldNumber;

        /// <summary>primaryIndexWorkspace (int32) @ 0x16. Runtime/workspace pointer - not stable across processes; 0 is safe for a freshly created file.</summary>
        internal int primaryIndexWorkspace;

        /// <summary>unknownPtr1A (int32) @ 0x1A. Runtime pointer, not stable across processes; 0 is safe for a freshly created file.</summary>
        internal int unknownPtr1A;

        /// <summary>pxRootBlockId (uint16) @ 0x1E. See <see cref="ParadoxHeaderOffsets.PxRootBlockId"/>; 0 for a freshly created empty .PX (no root block allocated yet).</summary>
        internal ushort pxRootBlockId;

        /// <summary>pxLevelCount (byte) @ 0x20. See <see cref="ParadoxHeaderOffsets.PxLevelCount"/>; 0 for a freshly created empty (zero-level) .PX.</summary>
        internal byte pxLevelCount;

        /// <summary>FieldCount (int16) @ 0x21. Number of field definitions that follow the header (for .PX files, this library's in-memory FieldCount is +3 for the trailing pointer fields - see <see cref="ReadHeader"/>).</summary>
        public short FieldCount { get; private set; }

        /// <summary>primaryKeyFields (int16) @ 0x23. Count of leading fields (of FieldCount) that make up the primary key; 0 when the table has no primary key.</summary>
        internal short primaryKeyFields;

        /// <summary>
        /// encryption1 (int32) @ 0x25. Holds the 32-bit encryption key for
        /// .DB files, or 0 when unencrypted. For index files (.PX/.Xnn/etc)
        /// this instead holds the sentinel 0xFF00FF00, meaning the real key
        /// lives in <see cref="V4Hdr.Encryption2"/> - see <see cref="EncryptionKey"/>.
        /// </summary>
        internal int encryption1;

        /// <summary>sortOrder (byte) @ 0x29. Language-driver/collation identifier byte; not currently written distinctly by this library (see sortOrderID string written separately for .DB files in <see cref="ParadoxHeaderBuilder"/>).</summary>
        internal byte sortOrder;

        /// <summary>modifiedFlags2 (byte) @ 0x2A. 0 for a freshly created file.</summary>
        internal byte modifiedFlags2;

        /// <summary>unknown2Bx2C (2 bytes) @ 0x2B-0x2C. Byte [1] (@ 0x2C) is a write counter - see <see cref="ParadoxHeaderOffsets.WriteCounter"/>; both bytes are runtime/process-dependent, not stable across independent creations.</summary>
        private byte[] unknown2Bx2C;  //  array[$002B..$002C] of byte;

        /// <summary>changeCount1 (byte) @ 0x2D. See <see cref="ParadoxHeaderOffsets.ChangeCount1"/>. Real BDE always stamps 0x5D here for .DB files (0 for .PX/.YGn/etc) - confirmed empirically; this appears to be a fixed stamp rather than content-derived.</summary>
        internal byte changeCount1;

        /// <summary>changeCount2 (byte) @ 0x2E. See <see cref="ParadoxHeaderOffsets.ChangeCount2"/>. Real BDE always stamps 0x5B here for .DB files (0 for .PX/.YGn/etc), paired with changeCount1 - confirmed empirically.</summary>
        internal byte changeCount2;

        /// <summary>unknown2F (byte) @ 0x2F. 0 for a freshly created file.</summary>
        internal byte unknown2F;

        /// <summary>tableNamePtrPtr (int32) @ 0x30. Runtime pointer, not stable across processes; 0 is safe for a freshly created file.</summary>
        private int tableNamePtrPtr; // ^pchar;

        /// <summary>fldInfoPtr (int32) @ 0x34. Runtime pointer, not stable across processes; 0 is safe for a freshly created file.</summary>
        private int fldInfoPtr;  //  PFldInfoRec;

        /// <summary>writeProtected (byte) @ 0x38. 0 for a freshly created, non-write-protected file.</summary>
        internal byte writeProtected;

        /// <summary>
        /// fileVersionID (byte) @ 0x39. The on-disk format level. This
        /// library defaults new tables to 0x0C (Paradox 7+, 261-byte "long"
        /// table-name field layout, needed to pair with the larger
        /// 32768-byte default block size - see <see cref="ParadoxHeaderBuilder.FileVersionId"/>);
        /// 0x0B is the older "short" 79-byte table-name layout (Paradox
        /// 5/7-compatible). The table-name field length and the presence of
        /// <see cref="V4Hdr"/> both depend on this byte - see <see cref="ReadHeader"/>.
        /// </summary>
        internal byte fileVersionID;

        /// <summary>maxBlocks (uint16) @ 0x3A. 0 for a freshly created empty file (no block limit reservation yet).</summary>
        internal ushort maxBlocks;

        /// <summary>unknown3C (byte) @ 0x3C. 0 for a freshly created file.</summary>
        internal byte unknown3C;

        /// <summary>auxPasswords (byte) @ 0x3D. 0 for a freshly created, unencrypted file.</summary>
        internal byte auxPasswords;

        /// <summary>unknown3Ex3F (2 bytes) @ 0x3E-0x3F. Real BDE .DB/.Xnn/.XGn files always have 0x0F1F here; .PX/.YGn files always have 0x0000 - confirmed empirically.</summary>
        private byte[] unknown3Ex3F; //  array[$003E..$003F] of byte;

        /// <summary>cryptInfoStartPtr (int32) @ 0x40. Runtime pointer, not stable across processes; 0 is safe for a freshly created, unencrypted file.</summary>
        private int cryptInfoStartPtr; //  pointer;

        /// <summary>cryptInfoEndPtr (int32) @ 0x44. Runtime pointer, not stable across processes; 0 is safe for a freshly created, unencrypted file.</summary>
        internal int cryptInfoEndPtr;

        /// <summary>unknown48 (byte) @ 0x48. 0 for a freshly created file.</summary>
        internal byte unknown48;

        /// <summary>autoIncVal (int32) @ 0x49. Next AUTOINC value to assign; 0 for a freshly created table with no AUTOINC field populated yet.</summary>
        internal int autoIncVal; //  longint;

        /// <summary>unknown4Dx4E (2 bytes) @ 0x4D-0x4E. 0 for a freshly created file.</summary>
        private byte[] unknown4Dx4E;  //array[$004D..$004E] of byte;

        /// <summary>indexUpdateRequired (byte) @ 0x4F. 0 for a freshly created table whose indexes (if any) are up to date.</summary>
        internal byte indexUpdateRequired;

        /// <summary>
        /// unknown50x54 (5 bytes) @ 0x50-0x54. Byte 0 (@ 0x50) has no stable
        /// pattern observed and is left zero. Bytes 1-2 (@ 0x51-0x52) are
        /// the CRITICAL, load-bearing realHeaderSize field (uint16) - the
        /// minimal byte count actually needed by the header fields + field
        /// defs + names (distinct from <see cref="headerSize"/>, which is
        /// always padded to a full 2048-byte block); zeroing this on an
        /// otherwise-valid file has been observed to crash the BDE engine
        /// on read. See <see cref="ParadoxHeaderBuilder"/>'s realHeaderSize
        /// computation (mirrors pxlibcpp's put_px_head()). Bytes 3-4 are
        /// left zero (only 1 real byte remains in that span after the
        /// uint16, per the builder's padding comments).
        /// </summary>
        internal byte[] unknown50x54;  //array[$0050..$0054] of byte;

        /// <summary>refIntegrity (byte) @ 0x55. 0 for a freshly created file (no referential-integrity constraints).</summary>
        private byte refIntegrity;

        /// <summary>unknown56x57 (2 bytes) @ 0x56-0x57. Real BDE .DB/.Xnn/.XGn files always have 0x0020 here; .PX/.YGn files always have 0x0000 - confirmed empirically, same hasV4Header-gated pattern as unknown3Ex3F above.</summary>
        internal byte[] unknown56x57;  //array[$0056..$0057] of byte;

        /// <summary>The optional 32-byte V4 header extension @ 0x58-0x77. Only present for .DB/.Xnn/.XGn files with fileVersionID &gt;= 5 - see <see cref="V4Hdr"/> for its own offsets/known values.</summary>
        internal V4Hdr V4Header;

        internal ParadoxFile.FieldInfo[] FieldTypes { get; set; } // array[1..255] of TFldInfoRec);

        /// <summary>tableNamePtr (int32). Runtime pointer immediately following the field definitions; not stable across processes, 0 is safe for a freshly created file.</summary>
        private int tableNamePtr;

        /// <summary>fieldNamePtrArray (one int32 per field). Runtime pointers, .DB files only; not stable across processes, 0 is safe for each entry in a freshly created file.</summary>
        private int[] fieldNamePtrArray;
        public string[] FieldNames { get; private set; }

        internal readonly Stream stream;
        private readonly BinaryReader reader;

        /// <summary>
        /// The 32-bit encryption key used to obfuscate this table's data blocks,
        /// or 0 if the table is not password-protected. Paradox stores this key
        /// (derived from the password via <see cref="PxCrypt.PasswordChecksum"/>)
        /// directly in the header, so decrypting a table's data does not
        /// require knowing the original password - this is why "master
        /// passwords" appear to work for any table: the key itself, not the
        /// password, gates the actual data.
        ///
        /// encryption1 (at header offset 0x25) holds the key for regular
        /// .DB files. For index files (.PX/.Xnn/etc.) that field instead
        /// holds the sentinel 0xFF00FF00, meaning the real key is stored in
        /// encryption2 within the V4 header block that follows the field
        /// definitions (only present when fileVersionID >= 5).
        /// </summary>
        public uint EncryptionKey
        {
            get
            {
                if (unchecked((uint)encryption1) == 0xFF00FF00 && V4Header != null)
                {
                    return unchecked((uint)V4Header.Encryption2);
                }
                return unchecked((uint)encryption1);
            }
        }

        public bool IsEncrypted => EncryptionKey != 0;

        /// <summary>
        /// Checks whether <paramref name="password"/> matches the password used to
        /// protect this table. If the table is not password-protected, this returns
        /// true only for a null or empty password.
        ///
        /// This library never gates reading or writing on a password - decrypting a
        /// table's data only requires the key stored in its own header (see
        /// <see cref="EncryptionKey"/>/<see cref="IsEncrypted"/>), not the original
        /// password itself. VerifyPassword/ChangePassword/RemovePassword are provided
        /// purely as optional conveniences for consumers who want to enforce their
        /// own password prompt/check before allowing access; they are not called
        /// anywhere internally. Check <see cref="IsEncrypted"/> to know up front
        /// whether a table is password-protected at all.
        /// </summary>
        public bool VerifyPassword(string password)
        {
            if (!IsEncrypted)
            {
                return string.IsNullOrEmpty(password);
            }
            return unchecked((uint)PxCrypt.PasswordChecksum(password ?? string.Empty)) == EncryptionKey;
        }

        /// <summary>
        /// Re-encrypts the table with a new password. <paramref name="currentPassword"/>
        /// must match the table's existing password (or be null/empty if the table is
        /// not currently password-protected). This is an optional operation for
        /// consumers that want to manage passwords explicitly; nothing in this
        /// library requires calling it.
        /// </summary>
        public void ChangePassword(string currentPassword, string newPassword)
        {
            if (!VerifyPassword(currentPassword))
            {
                throw new InvalidOperationException("The supplied current password is incorrect.");
            }
            uint newKey = string.IsNullOrEmpty(newPassword) ? 0u : unchecked((uint)PxCrypt.PasswordChecksum(newPassword));
            SetEncryptionKey(newKey);
        }

        /// <summary>
        /// Removes password protection from the table. <paramref name="currentPassword"/>
        /// must match the table's existing password. This is an optional operation
        /// for consumers that want to manage passwords explicitly; nothing in this
        /// library requires calling it.
        /// </summary>
        public void RemovePassword(string currentPassword)
        {
            if (!VerifyPassword(currentPassword))
            {
                throw new InvalidOperationException("The supplied current password is incorrect.");
            }
            SetEncryptionKey(0);
        }

        /// <summary>
        /// Decrypts every data block with the current key (if any) and re-encrypts
        /// with <paramref name="newKey"/> (0 meaning no encryption), then persists the
        /// new key to whichever header field currently supplies it.
        /// </summary>
        private void SetEncryptionKey(uint newKey)
        {
            uint oldKey = this.EncryptionKey;
            if (oldKey == newKey)
            {
                return;
            }

            int blockSize = this.maxTableSize * 0x0400;
            for (ushort blockNumber = 0; blockNumber < this.fileBlocks; blockNumber++)
            {
                long blockPosition = blockNumber * (long)blockSize + this.headerSize;
                var rawBlock = new byte[blockSize];
                this.stream.Position = blockPosition;
                int totalRead = 0;
                while (totalRead < blockSize)
                {
                    int n = this.stream.Read(rawBlock, totalRead, blockSize - totalRead);
                    if (n <= 0) break;
                    totalRead += n;
                }

                uint diskBlockNumber = (uint)(blockNumber + 1);
                if (oldKey != 0)
                {
                    PxCrypt.DecryptDbBlock(rawBlock, 0, blockSize, oldKey, diskBlockNumber);
                }
                if (newKey != 0)
                {
                    PxCrypt.EncryptDbBlock(rawBlock, 0, blockSize, newKey, diskBlockNumber);
                }

                this.stream.Position = blockPosition;
                this.stream.Write(rawBlock, 0, rawBlock.Length);
            }

            // encryption1 (header offset 0x25) holds the real key, unless it is the
            // 0xFF00FF00 sentinel, in which case the real key lives in encryption2
            // within the V4 header (offset 0x5C, right after fileVerID2/fileVerID3).
            if (unchecked((uint)this.encryption1) == 0xFF00FF00 && this.V4Header != null)
            {
                this.V4Header.encryption2 = unchecked((int)newKey);
                this.stream.Position = 0x5C;
            }
            else
            {
                this.encryption1 = unchecked((int)newKey);
                this.stream.Position = 0x25;
            }

            using (var writer = new BinaryWriter(new NonClosingStreamWrapper(this.stream), Encoding.Default))
            {
                writer.Write(unchecked((int)newKey));
            }
            this.stream.Flush();
        }

        public ParadoxFile(string filePath) : this(new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
        }

        public ParadoxFile(Stream stream)
        {
            this.stream = stream;
            this.reader = new BinaryReader(stream);
            stream.Position = 0;
            this.ReadHeader();
        }

        public virtual void Dispose()
        {
            this.stream?.Dispose();
        }

        internal virtual byte[] ReadBlob(byte[] blobInfo, int len, int hsize)
        {
            // TODO: implement this.
            return null;
        }

        internal virtual void WriteBlob(byte[] blobInfo, int len, int hsize, byte[] blobVal)
        {
            // TODO: implement this.
        }

        public IEnumerable<ParadoxReader.ParadoxRecord> Enumerate(Predicate<ParadoxReader.ParadoxRecord> where = null)
        {
            for (ushort blockNumber = 0; blockNumber < this.fileBlocks; blockNumber++)
            {
                var block = this.GetBlock(blockNumber);
                for (var recId = 0; recId < block.RecordCount; recId++)
                {
                    var rec = block[recId];
                    if (where == null || where(rec))
                    {
                        yield return rec;
                    }
                }
            }
        }

        /// <summary>
        /// Reads the base header (offsets 0x00-0x57), the optional V4 header
        /// extension (0x58-0x77, see <see cref="V4Hdr"/>), field definitions,
        /// table name, and field names, in that exact on-disk order. See the
        /// per-field summaries on this class's members above for each
        /// field's offset and known/expected values - this method is the
        /// authoritative reference for the read order and field sizes those
        /// summaries describe; <see cref="ParadoxHeaderBuilder"/> is the
        /// mirror-image from-scratch writer.
        /// </summary>
        private void ReadHeader()
        {
            var r = this.reader;
            RecordSize = r.ReadUInt16();
            headerSize = r.ReadUInt16();
            FileType = (ParadoxFileType) r.ReadByte();
            maxTableSize = r.ReadByte();
            RecordCount = r.ReadInt32();
            nextBlock = r.ReadUInt16();
            fileBlocks = r.ReadUInt16();
            firstBlock = r.ReadUInt16();
            lastBlock = r.ReadUInt16();
            unknown12x13 = r.ReadUInt16();
            modifiedFlags1 = r.ReadByte();
            indexFieldNumber = r.ReadByte();
            primaryIndexWorkspace = r.ReadInt32();
            unknownPtr1A = r.ReadInt32();
            pxRootBlockId = r.ReadUInt16();
            pxLevelCount = r.ReadByte();
            FieldCount = r.ReadInt16();
            primaryKeyFields = r.ReadInt16();
            encryption1 = r.ReadInt32();
            sortOrder = r.ReadByte();
            modifiedFlags2 = r.ReadByte();
            unknown2Bx2C = r.ReadBytes(0x002C - 0x002B + 1);
            changeCount1 = r.ReadByte();
            changeCount2 = r.ReadByte();
            unknown2F = r.ReadByte();
            tableNamePtrPtr = r.ReadInt32(); // ^pchar;
            fldInfoPtr = r.ReadInt32(); //  PFldInfoRec;
            writeProtected = r.ReadByte();
            fileVersionID = r.ReadByte();
            maxBlocks = r.ReadUInt16();
            unknown3C = r.ReadByte();
            auxPasswords = r.ReadByte();
            unknown3Ex3F = r.ReadBytes(0x003F - 0x003E + 1);
            cryptInfoStartPtr = r.ReadInt32(); //  pointer;
            cryptInfoEndPtr = r.ReadInt32();
            unknown48 = r.ReadByte();
            autoIncVal = r.ReadInt32(); //  longint;
            unknown4Dx4E = r.ReadBytes(0x004E - 0x004D + 1);
            indexUpdateRequired = r.ReadByte();
            unknown50x54 = r.ReadBytes(0x0054 - 0x0050 + 1);
            refIntegrity = r.ReadByte();
            unknown56x57 = r.ReadBytes(0x0057 - 0x0056 + 1);

            if ((this.FileType == ParadoxFileType.DbFileIndexed ||
                 this.FileType == ParadoxFileType.DbFileNotIndexed ||
                 this.FileType == ParadoxFileType.XnnFileInc ||
                 this.FileType == ParadoxFileType.XnnFileNonInc ||
                 this.FileType == ParadoxFileType.XgnFileInc ||
                 this.FileType == ParadoxFileType.XgnFileNonInc) &&
                this.fileVersionID >= 5)
            {
                this.V4Header = new V4Hdr(r);
            }
            var buff = new List<FieldInfo>();
            for (int i = 0; i < this.FieldCount; i++)
            {
                buff.Add(new FieldInfo(r));
            }
            if (this.FileType == ParadoxFileType.PxFile)
            {
                this.FieldCount += 3;
                buff.Add(new FieldInfo(ParadoxFieldTypes.Short, sizeof(short)));
                buff.Add(new FieldInfo(ParadoxFieldTypes.Short, sizeof(short)));
                buff.Add(new FieldInfo(ParadoxFieldTypes.Short, sizeof(short)));
            }
            this.FieldTypes = buff.ToArray();
            this.tableNamePtr = r.ReadInt32();
            if (this.FileType == ParadoxFileType.DbFileIndexed ||
                this.FileType == ParadoxFileType.DbFileNotIndexed)
            {
                fieldNamePtrArray = new int[this.FieldCount];
                for (int i = 0; i < this.FieldCount; i++)
                {
                    this.fieldNamePtrArray[i] = r.ReadInt32();
                }
            }
            var tableNameBuff = r.ReadBytes(this.fileVersionID >= 0x0C ? 261 : 79);
            this.TableName = Encoding.ASCII.GetString(tableNameBuff, 0, Array.FindIndex(tableNameBuff, b => b == 0));
            if (this.FileType == ParadoxFileType.DbFileIndexed ||
                this.FileType == ParadoxFileType.DbFileNotIndexed)
            {
                FieldNames = new string[this.FieldCount];
                for (int i = 0; i < this.FieldCount; i++)
                {
                    var fldNameBuff = new StringBuilder();
                    char ch;
                    while ((ch = r.ReadChar()) != '\x00') fldNameBuff.Append(ch);
                    this.FieldNames[i] = fldNameBuff.ToString();
                }
            }
        }

        internal DataBlock GetBlock(ushort blockNumber)
        {
            int blockSize = this.maxTableSize * 0x0400;
            long blockPosition = blockNumber * (long)blockSize + this.headerSize;
            this.stream.Position = blockPosition;

            if (this.IsEncrypted)
            {
                var rawBlock = new byte[blockSize];
                int totalRead = 0;
                while (totalRead < blockSize)
                {
                    int n = this.stream.Read(rawBlock, totalRead, blockSize - totalRead);
                    if (n <= 0) break;
                    totalRead += n;
                }
                // blockNumber on disk is 1-based for the crypt chunk salt.
                PxCrypt.DecryptDbBlock(rawBlock, 0, blockSize, this.EncryptionKey, (uint)(blockNumber + 1));
                using (var ms = new MemoryStream(rawBlock))
                using (var blockReader = new BinaryReader(ms))
                {
                    return new DataBlock(this, blockReader, blockNumber);
                }
            }

            return new DataBlock(this, this.reader, blockNumber);
        }

        private void WriteRecords(byte[] data, ushort blockNumber, int[] blockRecIndices)
        {
            int blockSize = this.maxTableSize * 0x0400;
            long blockPosition = blockNumber * (long)blockSize + this.headerSize;
            const int blockHeaderSize = sizeof(UInt16) + sizeof(UInt16) + sizeof(Int16); // nextBlock + blockNumber + addDataSize

            if (this.IsEncrypted)
            {
                // The cipher permutes bytes within each 256-byte chunk of the
                // physical block (header + records together), so individual
                // records cannot be safely overwritten in place on disk.
                // Instead: read the whole block back, decrypt it, splice in
                // the updated record bytes, then re-encrypt and rewrite the
                // whole block.
                var rawBlock = new byte[blockSize];
                this.stream.Position = blockPosition;
                int totalRead = 0;
                while (totalRead < blockSize)
                {
                    int n = this.stream.Read(rawBlock, totalRead, blockSize - totalRead);
                    if (n <= 0) break;
                    totalRead += n;
                }
                uint diskBlockNumber = (uint)(blockNumber + 1);
                PxCrypt.DecryptDbBlock(rawBlock, 0, blockSize, this.EncryptionKey, diskBlockNumber);

                foreach (var recIndex in blockRecIndices)
                {
                    Array.Copy(
                        data, recIndex * this.RecordSize,
                        rawBlock, blockHeaderSize + recIndex * this.RecordSize,
                        this.RecordSize);
                }

                PxCrypt.EncryptDbBlock(rawBlock, 0, blockSize, this.EncryptionKey, diskBlockNumber);
                this.stream.Position = blockPosition;
                this.stream.Write(rawBlock, 0, rawBlock.Length);
                return;
            }

            this.stream.Position = blockPosition + blockHeaderSize;

            using (var writer = new BinaryWriter(new NonClosingStreamWrapper(this.stream), Encoding.Default))
            {
                foreach (var recIndex in blockRecIndices)
                {
                    writer.Write(data, recIndex * this.RecordSize, this.RecordSize);
                }
            }
        }

        //public string GetString(byte[] data, int from, int maxLength)
        //{
        //    int dataLength = data.Length;
        //    int stringLength = Array.FindIndex(data, from, b => b == 0) - from;
        //    if (stringLength > maxLength)
        //        stringLength = maxLength;
        //    if (stringLength < 0)
        //        stringLength = 0;
        //    if (from < 0)
        //        from = 0;
        //    if ((from + stringLength) > dataLength)
        //        stringLength = dataLength;
        //    return Encoding.Default.GetString(data, from, stringLength);
        //}

        //public string GetStringFromMemo(byte[] data, int from, int size)
        //{
        //    var memoBufferSize = size - 10;
        //    var memoDataBuffer = new byte[memoBufferSize];
        //    var memoMetaData = new byte[10];
        //    Array.Copy(data, from, memoDataBuffer, 0, memoBufferSize);
        //    Array.Copy(data, from + memoBufferSize, memoMetaData, 0, 10);

        //    //var offsetIntoMemoFile = (long)BitConverter.ToInt32(memoMetaData, 0); 
        //    //offsetIntoMemoFile &= 0xffffff00;
        //    //var memoModNumber = BitConverter.ToInt16(memoMetaData,8); 
        //    //var index = memoMetaData[0]; 

        //    var memoSize = BitConverter.ToInt32(memoMetaData, 4);
        //    return GetString(memoDataBuffer, 0, memoSize);
        //}
    }
}
