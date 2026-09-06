using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    public partial class ParadoxFile : IDisposable
    {
        public string TableName;

        public ushort RecordSize { get; private set; }
        internal ushort headerSize;
        public ParadoxFileType FileType { get; private set; }
        internal byte maxTableSize;
        public int RecordCount { get; internal set; }
        internal ushort nextBlock;
        internal ushort fileBlocks;
        internal ushort firstBlock;
        internal ushort lastBlock;
        internal ushort unknown12x13;
        internal byte modifiedFlags1;
        internal byte indexFieldNumber;
        internal int primaryIndexWorkspace;
        internal int unknownPtr1A;
        internal ushort pxRootBlockId;
        internal byte pxLevelCount;
        public short FieldCount { get; private set; }
        internal short primaryKeyFields;
        internal int encryption1;
        internal byte sortOrder;
        internal byte modifiedFlags2;
        private byte[] unknown2Bx2C;  //  array[$002B..$002C] of byte;
        internal byte changeCount1;
        internal byte changeCount2;
        internal byte unknown2F;
        private int tableNamePtrPtr; // ^pchar;
        private int fldInfoPtr;  //  PFldInfoRec;
        internal byte writeProtected;
        internal byte fileVersionID;
        internal ushort maxBlocks;
        internal byte unknown3C;
        internal byte auxPasswords;
        private byte[] unknown3Ex3F; //  array[$003E..$003F] of byte;
        private int cryptInfoStartPtr; //  pointer;
        internal int cryptInfoEndPtr;
        internal byte unknown48;
        internal int autoIncVal; //  longint;
        private byte[] unknown4Dx4E;  //array[$004D..$004E] of byte;
        internal byte indexUpdateRequired;
        internal byte[] unknown50x54;  //array[$0050..$0054] of byte;
        private byte refIntegrity;
        internal byte[] unknown56x57;  //array[$0056..$0057] of byte;
        internal V4Hdr V4Header;
        internal ParadoxFile.FieldInfo[] FieldTypes { get; set; } // array[1..255] of TFldInfoRec);
        private int tableNamePtr;
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
