using System.Collections.Generic;

namespace ParadoxReader
{
    /// <summary>
    /// Represents a single B-tree node (block) in a Paradox index file
    /// (.PX, .Xnn, or .Xgn).
    /// </summary>
    internal class PxBlock
    {
        // ----------------------------------------------------------------
        // Properties
        // ----------------------------------------------------------------

        /// <summary>The block number of this node within the index file.</summary>
        public ushort BlockNumber { get; set; }

        /// <summary>
        /// Pointer to the leftmost child block.
        /// 0 if this is a leaf node.
        /// </summary>
        public ushort LeftChildBlockNumber { get; set; }

        /// <summary>The key entries stored in this node.</summary>
        public List<PxEntry> Entries { get; set; } = new List<PxEntry>();

        /// <summary>Maximum number of bytes available for entries in this block.</summary>
        public int Capacity { get; set; }

        // ----------------------------------------------------------------
        // Derived
        // ----------------------------------------------------------------

        /// <summary>True if this is a leaf node (no children).</summary>
        public bool IsLeaf => LeftChildBlockNumber == 0;

        /// <summary>Current used byte size of all entries combined.</summary>
        public int UsedSize
        {
            get
            {
                int size = 0;
                foreach (var entry in Entries)
                    size += entry.DiskSize;
                return size;
            }
        }

        /// <summary>True if there is no room for an entry of the given size.</summary>
        public bool IsFull(int entrySize) => (UsedSize + entrySize) > Capacity;
    }
}