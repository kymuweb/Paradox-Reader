using System;

namespace ParadoxReader
{
    /// <summary>
    /// Represents one data block in a Paradox .DB file.
    ///
    /// Block layout on disk:
    ///   Bytes [0..1]  nextBlock   (ushort) — next block number, 0 = none
    ///   Bytes [2..3]  blockNumber (ushort) — this block's own self-identifying
    ///                                        number (0-based, matches its
    ///                                        ordinal position in the file)
    ///   Bytes [4..5]  addDataSize (short)  — (recordCount - 1) * recordSize
    ///   Bytes [6..]   record data — recordCount * recordSize bytes
    ///
    /// Paradox blocks only chain forward via nextBlock; there is no
    /// back-pointer, so backward traversal is not possible.
    /// </summary>
    internal class DataBlock
    {
        // ----------------------------------------------------------------
        // Constants
        // ----------------------------------------------------------------

        /// <summary>Size in bytes of the block header.</summary>
        public const int HEADER_SIZE = 6;

        // ----------------------------------------------------------------
        // Header fields
        // ----------------------------------------------------------------

        public ushort BlockNumber  { get; set; }
        public ushort NextBlock    { get; set; }

        /// <summary>
        /// addDataSize = (RecordCount - 1) * RecordSize.
        /// A value of 0 means exactly 1 record is present.
        /// </summary>
        public short AddDataSize { get; set; }

        // ----------------------------------------------------------------
        // Layout info
        // ----------------------------------------------------------------

        public int    RecordSize   { get; }
        public int    BlockDataSize { get; } // usable bytes for records (excl. header)
        public byte[] RawData      { get; } // full block bytes (header + records)

        // ----------------------------------------------------------------
        // Derived
        // ----------------------------------------------------------------

        /// <summary>Number of records currently stored in this block.</summary>
        public int RecordCount
        {
            get => AddDataSize >= 0 ? (AddDataSize / RecordSize) + 1 : 0;
            set => AddDataSize = (short)((value - 1) * RecordSize);
        }

        /// <summary>Maximum records this block can hold.</summary>
        public int MaxRecords => BlockDataSize / RecordSize;

        /// <summary>True if there is room for at least one more record.</summary>
        public bool HasRoom => RecordCount < MaxRecords;

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public DataBlock(ushort blockNumber, int recordSize, int blockDataSize)
        {
            BlockNumber   = blockNumber;
            RecordSize    = recordSize;
            BlockDataSize = blockDataSize;
            RawData       = new byte[HEADER_SIZE + blockDataSize];

            // Default addDataSize to -recordSize so RecordCount returns 0
            // until at least one record is written.
            AddDataSize = (short)(-RecordSize);
        }

        // ----------------------------------------------------------------
        // Record slot access
        // ----------------------------------------------------------------

        /// <summary>Returns the byte offset of a record within RawData.</summary>
        public int RecordOffset(int recordIndex)
            => HEADER_SIZE + (recordIndex * RecordSize);

        /// <summary>Reads the raw bytes for the record at recordIndex.</summary>
        public byte[] GetRecordBytes(int recordIndex)
        {
            var bytes = new byte[RecordSize];
            Array.Copy(RawData, RecordOffset(recordIndex), bytes, 0, RecordSize);
            return bytes;
        }

        /// <summary>Writes raw bytes for the record at recordIndex.</summary>
        public void SetRecordBytes(int recordIndex, byte[] recordBytes)
        {
            int copyLen = Math.Min(recordBytes.Length, RecordSize);
            Array.Copy(recordBytes, 0, RawData, RecordOffset(recordIndex), copyLen);
        }

        // ----------------------------------------------------------------
        // Header serialization
        // ----------------------------------------------------------------

        /// <summary>Writes the header fields into RawData (call before writing to disk).</summary>
        public void FlushHeader()
        {
            RawData[0] = (byte)( NextBlock   & 0xFF);
            RawData[1] = (byte)( NextBlock   >> 8);
            RawData[2] = (byte)( BlockNumber & 0xFF);
            RawData[3] = (byte)( BlockNumber >> 8);
            RawData[4] = (byte)( AddDataSize & 0xFF);
            RawData[5] = (byte)((AddDataSize >> 8) & 0xFF);
        }

        /// <summary>Reads header fields from RawData (call after reading from disk).</summary>
        public void ParseHeader()
        {
            NextBlock   = (ushort)(RawData[0] | (RawData[1] << 8));
            BlockNumber = (ushort)(RawData[2] | (RawData[3] << 8));
            AddDataSize = (short) (RawData[4] | (RawData[5] << 8));
        }
    }
}
