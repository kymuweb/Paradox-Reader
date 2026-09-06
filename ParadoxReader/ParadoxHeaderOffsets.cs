namespace ParadoxReader
{
    /// <summary>
    /// Centralized physical byte offsets into a Paradox .DB/.PX header, as
    /// parsed by <see cref="ParadoxFile.ReadHeader"/>. These are shared by
    /// every class that needs to seek directly into the header region
    /// (<see cref="ParadoxTableFile"/>, <see cref="PrimaryIndexFile"/>,
    /// <see cref="TableRebuilder"/>, etc.) so the values are defined exactly
    /// once.
    /// </summary>
    /// <remarks>
    /// These offsets are for the table/index header layout observed across
    /// the versions supported by this library so far. If a future Paradox
    /// table version is found to lay the header out differently, that
    /// version's offsets should be added as a distinct, clearly-named set
    /// (e.g. behind a version check) rather than by mutating these values,
    /// so existing behavior for already-supported versions is preserved.
    /// </remarks>
    internal static class ParadoxHeaderOffsets
    {
        /// <summary>RecordSize (uint16) @ 0x00.</summary>
        public const int RecordSize = 0x00;

        /// <summary>headerSize (uint16) @ 0x02.</summary>
        public const int HeaderSize = 0x02;

        /// <summary>FileType (byte) @ 0x04.</summary>
        public const int FileType = 0x04;

        /// <summary>maxTableSize (byte) @ 0x05.</summary>
        public const int MaxTableSize = 0x05;

        /// <summary>RecordCount (int32) @ 0x06.</summary>
        public const int RecordCount = 0x06;

        /// <summary>
        /// Block-chain pointers: nextBlock, fileBlocks, firstBlock, lastBlock
        /// (uint16 x4, 8 bytes total) starting @ 0x0A.
        /// </summary>
        public const int BlockChain = 0x0A;

        /// <summary>pxRootBlockId (uint16) @ 0x1E.</summary>
        public const int PxRootBlockId = 0x1E;

        /// <summary>pxLevelCount (byte) @ 0x20.</summary>
        public const int PxLevelCount = 0x20;

        /// <summary>changeCount1 (byte) @ 0x2D.</summary>
        public const int ChangeCount1 = 0x2D;

        /// <summary>changeCount2 (byte) @ 0x2E.</summary>
        public const int ChangeCount2 = 0x2E;

        /// <summary>maxBlocks (uint16) @ 0x3A.</summary>
        public const int MaxBlocks = 0x3A;

        /// <summary>autoIncVal (int32) @ 0x49.</summary>
        public const int AutoIncVal = 0x49;

        /// <summary>Single-byte write counter (pxlib's unknown2Bx2C[1]) @ 0x2C.</summary>
        public const int WriteCounter = 0x2C;

        /// <summary>changeCount4 (short, V4Hdr) @ 0x70. Only present when the header region is large enough.</summary>
        public const int ChangeCount4 = 0x70;

        /// <summary>hasBlobFlag (byte) @ 0x74.</summary>
        public const int HasBlobFlag = 0x74;

        /// <summary>Blob (.MB) file global modification counter (uint16) @ 0x03, within the first 4096-byte header block.</summary>
        public const int BlobModCounter = 0x03;

        /// <summary>Size in bytes of a .MB blob file's header block.</summary>
        public const int BlobHeaderBlockSize = 4096;
    }
}
