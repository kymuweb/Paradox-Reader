using System;
using System.IO;

namespace ParadoxReader
{
    /// <summary>
    /// Handles low-level block read/write/allocate operations
    /// for a Paradox .DB file.
    /// </summary>
    internal class BlockManager
    {
        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly Stream stream;
        private readonly int    headerSize;
        private readonly int    recordSize;
        private readonly int    blockDataSize; // usable bytes per block (excl. block header)
        private readonly int    fullBlockSize; // blockDataSize + DataBlock.HEADER_SIZE
        private readonly uint   encryptionKey; // 0 if the table is not password-protected

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        /// <param name="stream">The .DB file stream.</param>
        /// <param name="headerSize">File header size in bytes.</param>
        /// <param name="recordSize">Size of one record in bytes.</param>
        /// <param name="maxTableSize">
        /// From the .DB file header. Block size = max(maxTableSize,1) * 1024.
        /// </param>
        /// <param name="encryptionKey">
        /// The table's 32-bit encryption key (see ParadoxFile.EncryptionKey),
        /// or 0 if the table is not password-protected.
        /// </param>
        public BlockManager(Stream stream, int headerSize, int recordSize, int maxTableSize, uint encryptionKey = 0)
        {
            this.stream     = stream;
            this.headerSize = headerSize;
            this.recordSize = recordSize;
            this.encryptionKey = encryptionKey;

            int tableSize      = Math.Max(maxTableSize, 1);
            this.fullBlockSize  = tableSize * 1024;
            this.blockDataSize  = fullBlockSize - DataBlock.HEADER_SIZE;
        }

        // ----------------------------------------------------------------
        // Properties
        // ----------------------------------------------------------------

        public int BlockDataSize => blockDataSize;
        public int RecordSize    => recordSize;

        // ----------------------------------------------------------------
        // Block I/O
        // ----------------------------------------------------------------

        /// <summary>
        /// Reads a block from disk by 0-based block number.
        /// </summary>
        public DataBlock ReadBlock(ushort blockNumber)
        {
            var block = new DataBlock(blockNumber, recordSize, blockDataSize);
            stream.Position = BlockPosition(blockNumber);
            int bytesRead = stream.Read(block.RawData, 0, block.RawData.Length);
            if (bytesRead < DataBlock.HEADER_SIZE)
                throw new IOException($"Failed to read block {blockNumber}: " +
                                      $"only {bytesRead} bytes read.");
            if (encryptionKey != 0)
            {
                // On-disk block numbers used for the crypt salt are 1-based.
                PxCrypt.DecryptDbBlock(block.RawData, 0, block.RawData.Length, encryptionKey, (uint)(blockNumber + 1));
            }
            block.ParseHeader();
            return block;
        }

        /// <summary>
        /// Writes a block back to disk.
        /// </summary>
        public void WriteBlock(DataBlock block)
        {
            block.FlushHeader();
            stream.Position = BlockPosition(block.BlockNumber);
            if (encryptionKey != 0)
            {
                var encrypted = (byte[])block.RawData.Clone();
                PxCrypt.EncryptDbBlock(encrypted, 0, encrypted.Length, encryptionKey, (uint)(block.BlockNumber + 1));
                stream.Write(encrypted, 0, encrypted.Length);
            }
            else
            {
                stream.Write(block.RawData, 0, block.RawData.Length);
            }
            stream.Flush();
        }

        /// <summary>
        /// Allocates a new block at the end of the file.
        /// The caller is responsible for linking it into the block chain
        /// (via NextBlock only \u2014 Paradox blocks have no back-pointer)
        /// and updating the file header.
        /// </summary>
        public DataBlock AllocateBlock()
        {
            long dataArea      = stream.Length - headerSize;
            ushort newBlockNum = (ushort)(dataArea / fullBlockSize); // 0-based

            // Extend the file to accommodate the new block
            stream.SetLength(headerSize + ((newBlockNum + 1) * (long)fullBlockSize));

            var block = new DataBlock(newBlockNum, recordSize, blockDataSize);
            block.NextBlock   = 0;
            block.RecordCount = 0;

            // Zero-initialise and flush to disk
            WriteBlock(block);
            return block;
        }

        // ----------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------

        /// <summary>
        /// Returns the byte position in the stream for a 0-based block number.
        /// </summary>
        private long BlockPosition(ushort blockNumber)
            => headerSize + (blockNumber * (long)fullBlockSize);
    }
}
