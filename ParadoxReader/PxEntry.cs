namespace ParadoxReader
{
    /// <summary>
    /// Represents a single entry in a Paradox B-tree node (.PX or .Xnn file).
    ///
    /// Empirically decoded on-disk leaf entry format (via SQLRunner byte-level
    /// comparison): leaf entries are per-DB-BLOCK, not per-row. Each leaf entry
    /// holds the key of the FIRST row in that .DB data block, plus a 6-byte
    /// pointer: blockNumber(ushort) + recordCount(ushort) + reserved(ushort),
    /// all individually sign-flip encoded (value ^ 0x8000) and stored
    /// big-endian, same as KeySerializer's Short field encoding. Internal
    /// (branch) node entries reuse the same 6-byte pointer shape, with
    /// BlockNumber pointing at the child index block and RecordCount unused (0).
    /// </summary>
    internal class PxEntry
    {
        /// <summary>Raw serialized bytes of the key fields.</summary>
        public byte[] KeyData { get; set; }

        /// <summary>
        /// For internal nodes: block number of the right child in the index file.
        /// For leaf nodes:     block number in the .DB file whose first row's key
        ///                     is stored in KeyData.
        /// </summary>
        public ushort BlockNumber { get; set; }

        /// <summary>
        /// For leaf nodes: number of records currently stored in the referenced
        /// .DB data block. Unused (0) for internal/branch node entries.
        /// </summary>
        public ushort RecordCount { get; set; }

        public PxEntry(byte[] keyData, ushort blockNumber, ushort recordCount)
        {
            KeyData     = keyData;
            BlockNumber = blockNumber;
            RecordCount = recordCount;
        }

        /// <summary>
        /// Total size of this entry on disk: key data bytes + 2 bytes block
        /// number + 2 bytes record count + 2 reserved bytes.
        /// </summary>
        public int DiskSize => KeyData.Length + 6;

        /// <summary>
        /// Sign-flip encodes/decodes a pointer-field word for on-disk storage.
        /// XOR with 0x8000 is self-inverse, so the same function both encodes
        /// and decodes.
        /// </summary>
        public static ushort FlipWord(ushort value) => (ushort)(value ^ 0x8000);
    }
}