using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Read-side traversal of a Paradox primary key (.PX) B-tree index.
    ///
    /// IMPORTANT: The on-disk .PX B-tree node layout is NOT the same as a
    /// .DB data block. Each node's 6-byte header is
    /// leftChildBlockNumber(ushort) + reserved(ushort) + usedBytes(ushort),
    /// followed by entries of keyData + a 6-byte pointer
    /// (blockNumber(ushort) + recordCount(ushort) + reserved(ushort)), each
    /// word sign-flip encoded (value ^ 0x8000) and stored big-endian. Block
    /// numbers within the .PX file are 1-based. This mirrors the layout
    /// documented and used for writes in <see cref="PrimaryIndexFile"/> and
    /// for reads in <see cref="SecondaryIndexFile.Enumerate"/>.
    /// </summary>
    public class ParadoxPrimaryKey : ParadoxFile
    {
        private const int POINTER_SIZE = 6; // blockNumber(2) + recordCount(2) + reserved(2)
        private const int HEADER_SIZE  = 6; // leftChild(2) + reserved(2) + usedBytes(2)

        private readonly ParadoxFile table;
        private readonly ParadoxFile.FieldInfo[] primaryKeyFieldsArray;
        private readonly int keyDataSize;
        private readonly int entrySize;
        private readonly int blockCapacity;

        public ParadoxPrimaryKey(ParadoxFile table, string filePath)
            : base(filePath)
        {
            this.table = table;
            this.FilePath = filePath;

            this.primaryKeyFieldsArray = table.FieldTypes.Take(table.primaryKeyFields).ToArray();
            foreach (var f in this.primaryKeyFieldsArray)
                keyDataSize += f.fSize;

            entrySize     = keyDataSize + POINTER_SIZE;
            blockCapacity = this.maxTableSize * 0x400 - HEADER_SIZE;

            // BDE/Pdxrbld considers an index out of date if its own
            // autoIncVal (offset 0x49) doesn't match the parent .DB's after
            // an AutoInc field is assigned. Flag it here (without throwing)
            // so callers can check IsOutOfDate; Enumerate still throws if
            // actually used.
            if (table.autoIncVal != 0 && this.autoIncVal != table.autoIncVal)
                IsOutOfDate = true;
        }

        /// <summary>
        /// True if this index's autoIncVal didn't match the parent .DB's at
        /// open time. Attempting to <see cref="Enumerate"/> while this is
        /// true throws <see cref="IndexOutOfDateException"/>.
        /// </summary>
        public bool IsOutOfDate { get; }

        /// <summary>Full path to this .PX file.</summary>
        public string FilePath { get; }

        // Test/diagnostic-only observability into the .PX B-tree depth, used
        // to confirm when the index has grown beyond a single (leaf-only)
        // level. Not used by any production read/write logic.
        public int LevelCount => this.pxLevelCount;
        public int RootBlockId => this.pxRootBlockId;

        public IEnumerable<ParadoxReader.ParadoxRecord> Enumerate(ParadoxCondition condition)
        {
            if (IsOutOfDate)
            {
                throw new IndexOutOfDateException(this.FilePath,
                    $"Index is out of date: '{this.FilePath}'. Consider TableRebuilder.Rebuild " +
                    "to regenerate the index.");
            }

            if (this.stream.Length <= this.headerSize) yield break;

            foreach (var rec in EnumerateNode(ReadBlock(this.pxRootBlockId), condition, this.pxLevelCount))
                yield return rec;
        }

        /// <summary>
        /// Recursively traverses a .PX B-tree node. <paramref name="level"/> is
        /// the node's depth, per the file header's pxLevelCount (root starts
        /// at pxLevelCount). Empirically (against a real 3-level
        /// TESTTAB_LARGEDATA.DB index), Entries alone fully and exclusively
        /// enumerate a node's children at every level - LeftChildBlockNumber
        /// is redundant/non-child data (e.g. duplicates Entries[0].BlockNumber
        /// on the root) and must never be followed, or rows get double
        /// counted / traversal walks into the wrong level. The bottom PX
        /// level (level == 1) is where entries directly reference .DB blocks
        /// rather than further .PX blocks.
        /// </summary>
        private IEnumerable<ParadoxReader.ParadoxRecord> EnumerateNode(PxBlock node, ParadoxCondition condition, int level)
        {
            bool isLeaf = level <= 1;
            if (isLeaf)
            {
                for (int i = 0; i < node.Entries.Count; i++)
                {
                    var entry     = node.Entries[i];
                    var nextEntry = i < node.Entries.Count - 1 ? node.Entries[i + 1] : null;
                    var indexRec  = BuildSyntheticRecord(entry);
                    var nextRec   = nextEntry != null ? BuildSyntheticRecord(nextEntry) : null;

                    if (!condition.IsIndexPossible(indexRec, nextRec)) continue;

                    // Leaf entries store the .DB block number 1-based, but
                    // ParadoxFile.GetBlock indexes blocks 0-based, so convert here.
                    //
                    // A rebuilt/shrunk .DB can leave a primary index leaf entry
                    // pointing at a block number that no longer exists in the .DB
                    // file. Skip stale entries instead of crashing with an
                    // uncaught EndOfStreamException.
                    int dbBlockNumber = entry.BlockNumber - 1;
                    if (dbBlockNumber < 0 || dbBlockNumber >= this.table.fileBlocks)
                        continue;

                    var block = this.table.GetBlock((ushort)dbBlockNumber);
                    for (int r = 0; r < block.RecordCount; r++)
                    {
                        var rec = block[r];
                        if (condition.IsDataOk(rec)) yield return rec;
                    }
                }
                yield break;
            }

            for (int i = 0; i < node.Entries.Count; i++)
            {
                var entry     = node.Entries[i];
                var nextEntry = i < node.Entries.Count - 1 ? node.Entries[i + 1] : null;
                var indexRec  = BuildSyntheticRecord(entry);
                var nextRec   = nextEntry != null ? BuildSyntheticRecord(nextEntry) : null;

                if (!condition.IsIndexPossible(indexRec, nextRec)) continue;

                foreach (var rec in EnumerateNode(ReadBlock(entry.BlockNumber), condition, level - 1))
                    yield return rec;
            }
        }

        /// <summary>
        /// Builds a synthetic <see cref="ParadoxRecord"/> from a B-tree leaf/
        /// branch entry's key data, so <see cref="ParadoxCondition"/> can
        /// evaluate it via the same DataValues-indexed API used for real
        /// table rows.
        /// </summary>
        private ParadoxReader.ParadoxRecord BuildSyntheticRecord(PxEntry entry)
        {
            var values = KeySerializer.Deserialize(entry.KeyData, primaryKeyFieldsArray);
            return new ParadoxReader.ParadoxRecord(entry.BlockNumber, 0, values);
        }

        private PxBlock ReadBlock(ushort blockNumber)
        {
            var block = new PxBlock
            {
                BlockNumber = blockNumber,
                Capacity    = blockCapacity
            };
            int  blockSize = this.maxTableSize * 0x400;
            long pos       = this.headerSize + (long)(blockNumber - 1) * blockSize;

            if (pos < this.headerSize || pos + blockSize > this.stream.Length)
                throw new System.InvalidOperationException(
                    $"[ParadoxPrimaryKey.ReadBlock] Block {blockNumber} is out of range. " +
                    $"offset={pos}, blockSize={blockSize}, " +
                    $"streamLength={this.stream.Length}, headerSize={this.headerSize}.");

            this.stream.Position = pos;
            using (var r = new BinaryReader(new NonClosingStreamWrapper(this.stream), Encoding.Default))
            {
                block.LeftChildBlockNumber = r.ReadUInt16();
                r.ReadUInt16(); // reserved
                ushort usedBytes = r.ReadUInt16();

                // usedBytes = (entryCount - 1) * entrySize, per PrimaryIndexFile.WriteBlock
                // (0 for both 0 and 1 entries, but any block reached via traversal is
                // referenced by a parent entry, so it always has at least 1 entry).
                //
                // A rebuilt/stale block can leave garbage in usedBytes (observed on
                // real corpus data, e.g. !SECURIT.PX after a BDE rebuild), which
                // would otherwise compute an entryCount that reads far past this
                // block's actual capacity and crash with an EndOfStreamException.
                // Clamp to what the block can physically hold.
                int entryCount = usedBytes == 0 ? 1 : (usedBytes / entrySize) + 1;
                int maxEntries = blockCapacity / entrySize;
                if (entryCount < 1 || entryCount > maxEntries) entryCount = Math.Min(Math.Max(entryCount, 1), Math.Max(maxEntries, 1));

                for (int i = 0; i < entryCount; i++)
                {
                    byte[] key = r.ReadBytes(keyDataSize);
                    ushort bn = ReadSignFlippedUInt16(r);
                    ushort rc = ReadSignFlippedUInt16(r);
                    r.ReadBytes(2); // reserved word
                    block.Entries.Add(new PxEntry(key, bn, rc));
                }
            }
            return block;
        }

        private static ushort ReadSignFlippedUInt16(BinaryReader r)
        {
            byte hi = r.ReadByte();
            byte lo = r.ReadByte();
            ushort encoded = (ushort)((hi << 8) | lo);
            return PxEntry.FlipWord(encoded);
        }
    }
}
