using System;
using System.Collections.Generic;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// Maps a global, zero-based grid row index to the Paradox data block and
    /// in-block record slot that holds it, without decoding every record in
    /// the table up front. This is what allows <see cref="TableEditorForm"/>
    /// to use <see cref="System.Windows.Forms.DataGridView"/> virtual mode:
    /// only the rows the grid actually needs to display are decoded, so
    /// opening/scrolling a table with millions of records is just as fast as
    /// opening a small one.
    ///
    /// Paradox stores records in a forward-only chain of fixed-size blocks
    /// (see <see cref="ParadoxFile"/>), each holding a variable number of
    /// records (up to the block's capacity). There is no global row index on
    /// disk, so this cursor builds a small in-memory table of cumulative
    /// record counts per block (cheap: one block-header read per block, not
    /// per record) and uses that to binary-search from a row index to
    /// (blockNumber, recordIndex) in O(log blockCount). Decoded
    /// <see cref="ParadoxRecord"/> instances are cached per-block (bounded by
    /// a small LRU of recently-used blocks) so repeated access to the same
    /// visible rows (e.g. during scrolling) doesn't repeatedly hit disk.
    /// </summary>
    internal sealed class RowCursor
    {
        private struct BlockRange
        {
            public ushort BlockNumber;
            public int StartRow;   // global row index of this block's first record
            public int RecordCount;
        }

        private const int MaxCachedBlocks = 8;

        private readonly ParadoxTableFile table;
        private List<BlockRange> blockRanges;
        private int totalRows;

        // Small LRU-ish cache of decoded blocks (as ParadoxRecord[] arrays) keyed by block number.
        private readonly LinkedList<ushort> blockCacheOrder = new LinkedList<ushort>();
        private readonly Dictionary<ushort, ParadoxRecord[]> blockCache = new Dictionary<ushort, ParadoxRecord[]>();

        public int TotalRows => totalRows;

        public RowCursor(ParadoxTableFile table)
        {
            this.table = table;
            Refresh();
        }

        /// <summary>
        /// Rebuilds the block/row map and clears the record cache. Call this
        /// after any operation that changes the table's block layout
        /// (insert, delete, rebuild).
        /// </summary>
        public void Refresh()
        {
            blockCache.Clear();
            blockCacheOrder.Clear();

            blockRanges = new List<BlockRange>();
            int row = 0;
            for (ushort blockNumber = 0; blockNumber < table.fileBlocks; blockNumber++)
            {
                var block = table.GetBlock(blockNumber);
                int count = block.RecordCount;
                if (count <= 0) continue;

                blockRanges.Add(new BlockRange
                {
                    BlockNumber = blockNumber,
                    StartRow = row,
                    RecordCount = count,
                });
                row += count;
            }

            totalRows = row;
        }

        /// <summary>Returns the decoded record for the given global row index, or null if out of range.</summary>
        public ParadoxRecord GetRecord(int rowIndex)
        {
            if (rowIndex < 0 || rowIndex >= totalRows) return null;

            int rangeIndex = FindBlockRange(rowIndex);
            if (rangeIndex < 0) return null;

            var range = blockRanges[rangeIndex];
            int inBlockIndex = rowIndex - range.StartRow;

            var records = GetOrDecodeBlock(range.BlockNumber);
            if (inBlockIndex < 0 || inBlockIndex >= records.Length) return null;

            return records[inBlockIndex];
        }

        /// <summary>Binary-searches blockRanges for the range containing rowIndex.</summary>
        private int FindBlockRange(int rowIndex)
        {
            int lo = 0, hi = blockRanges.Count - 1;
            while (lo <= hi)
            {
                int mid = lo + (hi - lo) / 2;
                var range = blockRanges[mid];
                if (rowIndex < range.StartRow) hi = mid - 1;
                else if (rowIndex >= range.StartRow + range.RecordCount) lo = mid + 1;
                else return mid;
            }
            return -1;
        }

        private ParadoxRecord[] GetOrDecodeBlock(ushort blockNumber)
        {
            ParadoxRecord[] records;
            if (blockCache.TryGetValue(blockNumber, out records))
            {
                blockCacheOrder.Remove(blockNumber);
                blockCacheOrder.AddLast(blockNumber);
                return records;
            }

            var block = table.GetBlock(blockNumber);
            records = new ParadoxRecord[block.RecordCount];
            for (int i = 0; i < records.Length; i++)
                records[i] = block[i];

            blockCache[blockNumber] = records;
            blockCacheOrder.AddLast(blockNumber);

            if (blockCache.Count > MaxCachedBlocks)
            {
                var oldest = blockCacheOrder.First.Value;
                blockCacheOrder.RemoveFirst();
                blockCache.Remove(oldest);
            }

            return records;
        }
    }
}
