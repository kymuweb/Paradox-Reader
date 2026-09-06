using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Manages a single secondary index file (.Xnn or .Xgn) for a Paradox table.
    /// Uses the same B-tree algorithm as PrimaryIndex but indexes
    /// non-primary-key fields, identified by their position in the parent table.
    ///
    /// IMPORTANT: Empirically decoded via SQLRunner byte-level comparison
    /// (see project history), Paradox leaf entries are per-DB-BLOCK, not
    /// per-row. Each leaf entry stores the key of the FIRST row currently
    /// in a given .DB data block, plus a 6-byte pointer:
    /// blockNumber(ushort) + recordCount(ushort) + reserved(ushort), each
    /// sign-flip encoded (value ^ 0x8000) and written big-endian (same
    /// encoding KeySerializer uses for Short fields). The block header is
    /// 6 bytes: leftChildBlockNumber(ushort) + reserved(ushort) +
    /// usedBytes(ushort), where usedBytes = (entryCount - 1) * entrySize.
    /// </summary>
    internal class SecondaryIndexFile : IDisposable
    {
        // ----------------------------------------------------------------
        // Constants
        // ----------------------------------------------------------------

        private const int POINTER_SIZE = 6; // blockNumber(2) + recordCount(2) + reserved(2)
        private const int HEADER_SIZE  = 6; // leftChild(2) + reserved(2) + usedBytes(2)

        // Real-world Paradox B-tree indexes are shallow (a handful of levels even
        // for tables with tens of thousands of records). A corrupt or cyclic index
        // file (observed on real corpus data, e.g. !itemfee/!REFERAL) can otherwise
        // send recursive tree-walk helpers (InsertNonFull, DeleteFromNode,
        // GetPredecessor, EnumerateNode) into unbounded/cyclic recursion, which -
        // unlike a normal exception - crashes the whole process with an
        // uncatchable StackOverflowException. Bail out with a catchable exception
        // once this depth is exceeded instead.
        private const int MAX_TREE_DEPTH = 64;

        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly ParadoxFile indexFile;
        private readonly ParadoxFile.FieldInfo[] indexedFields;
        private readonly int[]       fieldIndices;  // 0-based positions in parent table
        private readonly int         keyDataSize;
        private readonly int         entrySize;
        private readonly int         pointerSize;   // bytes per entry pointer; varies by index type (2 or 6 observed)
        private readonly int         blockCapacity;
        // XgnFile types use 0-based block numbers; all others (YgnFile, XnnFile) use 1-based.
        private readonly ushort      blockBase;

        /// <summary>
        /// Full path to the underlying .Xnn/.Xgn/.Ynn/.Ygn index file.
        /// </summary>
        public string FilePath { get; }

        /// <summary>
        /// 0-based field positions in the parent table that this index's
        /// composed key covers, in key order (indexed field(s) followed by
        /// appended primary-key field(s)). Use these positions - not the
        /// parent table's own field positions - when building a
        /// <see cref="ParadoxCondition.Compare"/>'s IndexFieldIndex for use
        /// with <see cref="Enumerate"/>.
        /// </summary>
        public int[] FieldIndices => fieldIndices;

        /// <summary>This index's own RecordCount header field (offset 0x06).</summary>
        public int RecordCount => indexFile.RecordCount;

        /// <summary>This index's own autoIncVal header field (offset 0x49).</summary>
        public int AutoIncVal => indexFile.autoIncVal;

        /// <summary>
        /// True once <see cref="MarkOutOfDate"/> has been called (this
        /// index's autoIncVal didn't match the parent .DB's at open time).
        /// When set, any attempt to read/write via this index throws
        /// <see cref="IndexOutOfDateException"/>.
        /// </summary>
        public bool IsOutOfDate { get; private set; }

        /// <summary>
        /// Flags this index as out of date. Called by <see cref="IndexManager"/>
        /// right after opening, if this index's autoIncVal didn't match the
        /// parent .DB's. Does not itself throw, so callers can inspect
        /// <see cref="IsOutOfDate"/>/<c>ParadoxTableFile.IndexOutOfDate</c>
        /// without an exception; actual index reads/writes still throw.
        /// </summary>
        internal void MarkOutOfDate() => IsOutOfDate = true;

        private void ThrowIfOutOfDate()
        {
            if (IsOutOfDate)
            {
                throw new IndexOutOfDateException(FilePath,
                    $"Index is out of date: '{FilePath}'. Consider TableRebuilder.Rebuild " +
                    "to regenerate the index.");
            }
        }

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        internal SecondaryIndexFile(string indexFilePath, ParadoxFile.FieldInfo[] indexedFields, int[] fieldIndices)
        {
            FilePath           = indexFilePath;
            this.indexedFields = indexedFields;
            this.fieldIndices  = fieldIndices;

            foreach (var f in indexedFields)
                keyDataSize += f.fSize;

            indexFile     = new ParadoxFile(indexFilePath);

            // The entry pointer width varies by index file (2 bytes observed
            // for some .Xgn/.Ygn files, 6 bytes - blockNumber + recordCount +
            // reserved - for others), so derive it from the file's own
            // on-disk RecordSize rather than assuming POINTER_SIZE (6).
            entrySize     = indexFile.RecordSize > keyDataSize ? indexFile.RecordSize : keyDataSize + POINTER_SIZE;
            pointerSize   = entrySize - keyDataSize;
            blockCapacity = indexFile.maxTableSize * 0x400 - HEADER_SIZE;

            // XgnFile types are documented/typically 0-based block numbers while
            // YgnFile/XnnFile are 1-based, but this has been observed to NOT hold
            // for every real-world file (e.g. a corpus !account.X12 - nominally
            // XnnFileInc/1-based - whose on-disk pxRootBlockId only resolves to a
            // valid in-file position under the 0-based interpretation). Rather than
            // trust the file-type-derived default blindly (which can compute a
            // negative/out-of-range stream position and throw
            // ArgumentOutOfRangeException from FileStream.set_Position), verify it
            // against the file's own root block pointer and fall back to whichever
            // interpretation is actually consistent with the on-disk data.
            ushort typeDefaultBase = (indexFile.FileType == ParadoxFileType.XgnFileNonInc ||
                                      indexFile.FileType == ParadoxFileType.XgnFileInc) ? (ushort)0 : (ushort)1;
            blockBase = ResolveBlockBase(typeDefaultBase, indexFile);

            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile] Opened '{indexFilePath}': " +
                $"rootBlock={indexFile.pxRootBlockId}, headerSize={indexFile.headerSize}, " +
                $"maxTableSize={indexFile.maxTableSize}, blockSize={indexFile.maxTableSize * 0x400}, " +
                $"streamLength={indexFile.stream.Length}, keyDataSize={keyDataSize}, " +
                $"entrySize={entrySize}, pointerSize={pointerSize}, blockCapacity={blockCapacity}, " +
                $"blockBase={blockBase}, indexFieldNumber={indexFile.indexFieldNumber}, " +
                $"pxLevelCount={indexFile.pxLevelCount}, " +
                $"fieldIndices=[{string.Join(",", Array.ConvertAll<int, string>(fieldIndices, i => i.ToString()))}]");
        }

        // ----------------------------------------------------------------
        // Public API
        // ----------------------------------------------------------------

        /// <summary>
        /// Called whenever a .DB data block's contents change (a record was
        /// inserted, updated, or deleted in that block). This is the ONLY
        /// entry point index maintenance needs, because Paradox leaf entries
        /// track the whole block, not individual rows: given the current key
        /// of the block's first row (using this index's own fields) and its
        /// current record count, the correct leaf entry can always be derived.
        /// </summary>
        /// <param name="firstRowAllFieldValues">
        /// All field values of the record at index 0 in the block, or null
        /// if the block is now empty (recordCount == 0).
        /// </param>
        /// <param name="blockNumber">The affected .DB block number.</param>
        /// <param name="recordCount">The block's current record count.</param>
        public void OnBlockChanged(object[] firstRowAllFieldValues, ushort blockNumber, int recordCount)
        {
            ThrowIfOutOfDate();

            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile.OnBlockChanged] blockNumber={blockNumber}, recordCount={recordCount}");

            // Leaf entries store the .DB block number 1-based (matching the
            // real on-disk format confirmed against TESTTAB_LARGEDATA.DB/.PX), but
            // blockNumber (from ParadoxTableFile) is 0-based, so convert here.
            ushort dbBlockNumber = (ushort)(blockNumber + 1);

            if (recordCount <= 0)
            {
                RemoveBlockEntry(dbBlockNumber);
                return;
            }

            byte[] keyData = KeySerializer.Serialize(ExtractIndexValues(firstRowAllFieldValues), indexedFields);

            if (indexFile.stream.Length <= indexFile.headerSize || indexFile.RecordCount <= 0)
            {
                var newLeaf = AllocateBlock();
                newLeaf.Entries.Add(new PxEntry(keyData, dbBlockNumber, (ushort)recordCount));
                WriteBlock(newLeaf);
                UpdateRootBlockId(newLeaf.BlockNumber);
                UpdateLevelCount(1);
                UpdateRecordCount(indexFile.RecordCount + 1);
                return;
            }

            var existing = FindEntryForBlock(dbBlockNumber);
            if (existing.Block != null)
            {
                var entry = existing.Block.Entries[existing.Index];
                entry.KeyData     = keyData;
                entry.RecordCount = (ushort)recordCount;
                WriteBlock(existing.Block);
                return;
            }

            BTreeInsert(new PxEntry(keyData, dbBlockNumber, (ushort)recordCount));
            UpdateRecordCount(indexFile.RecordCount + 1);
        }

        /// <summary>
        /// Traverses this secondary index's B-tree to find matching records
        /// in the parent .DB table, analogous to
        /// <see cref="ParadoxPrimaryKey.Enumerate(ParadoxCondition)"/> for
        /// the primary (.PX) index. <paramref name="condition"/>'s
        /// <see cref="ParadoxCondition.Compare.IndexFieldIndex"/> must refer
        /// to the position of the target field within this index's own
        /// composed key (see <see cref="FieldIndices"/>), not its position
        /// in the parent table.
        /// </summary>
        /// <param name="condition">Condition used both to prune index branches (IsIndexPossible) and to filter matching rows (IsDataOk).</param>
        /// <param name="table">The parent .DB table file, used to read the data blocks referenced by leaf entries.</param>
        public IEnumerable<ParadoxReader.ParadoxRecord> Enumerate(ParadoxCondition condition, ParadoxFile table)
        {
            ThrowIfOutOfDate();

            if (indexFile.stream.Length <= indexFile.headerSize) yield break;
            if (indexFile.RecordCount <= 0) yield break;

            foreach (var rec in EnumerateNode(ReadBlock(indexFile.pxRootBlockId), condition, table, null))
                yield return rec;
        }

        /// <summary>
        /// Recursively traverses this secondary index's B-tree node.
        ///
        /// IMPORTANT: unlike the primary key (.PX) index, this index file's
        /// own pxLevelCount header field was empirically found to be
        /// unreliable (observed as 0 on a real multi-level TESTTAB_LARGEDATA.XG0
        /// index), so leaf detection cannot rely on level counting the way
        /// <see cref="ParadoxPrimaryKey.EnumerateNode"/> does. Instead, since
        /// the bottom level's entries reference .DB blocks (which can be
        /// numbered arbitrarily higher than this index file's own block
        /// count) while every other level's entries reference in-range
        /// index blocks, a node is treated as a leaf when its entries
        /// reference blocks outside this index file's own valid block
        /// range.
        ///
        /// A single logical tree node can span multiple physical blocks:
        /// when a node overflows one block, LeftChildBlockNumber chains to
        /// a *sibling* block holding the node's remaining entries, in
        /// strictly ascending key order (confirmed empirically on
        /// TESTTAB_LARGEDATA.XG0: root block 0's own entries end at key
        /// "009D8080...", and its LeftChildBlockNumber=2 chains to a block
        /// whose entries continue upward from "00A08080..."; this repeats
        /// at every level, not just leaves). So LeftChildBlockNumber must
        /// be followed at every node level, treating the whole chain's
        /// entries as one continuous ascending sequence for pruning
        /// purposes, rather than treated as a separate "lesser" subtree.
        ///
        /// <paramref name="upperBoundRec"/> is the synthetic record for the
        /// smallest key known (from an ancestor) to be greater than every
        /// key in this entire node's subtree, or null if no such bound is
        /// known (rightmost spine of the tree). It must be threaded down as
        /// the fallback "next" record whenever a node's own last entry has
        /// no following sibling within the node/chain, or IsIndexPossible
        /// would incorrectly treat the last entry as an open-ended
        /// (infinite) upper range and always descend into it.
        /// </summary>
        private IEnumerable<ParadoxReader.ParadoxRecord> EnumerateNode(
            PxBlock node, ParadoxCondition condition, ParadoxFile table, ParadoxReader.ParadoxRecord upperBoundRec, int depth = 0)
        {
            if (depth > MAX_TREE_DEPTH)
                throw new InvalidOperationException(
                    $"[SecondaryIndexFile.EnumerateNode] Exceeded max tree depth ({MAX_TREE_DEPTH}) for '{FilePath}'; " +
                    "the index file appears to be corrupt or cyclic. Consider using TableRebuilder.Rebuild to rebuild the table and its indexes.");

            PxBlock cur = node;
            var visited = new HashSet<ushort>();
            while (cur != null)
            {
                if (!visited.Add(cur.BlockNumber))
                    throw new InvalidOperationException(
                        $"[SecondaryIndexFile.EnumerateNode] Detected a cyclic leaf/sibling chain in '{FilePath}' " +
                        $"(block {cur.BlockNumber} revisited); the index file appears to be corrupt. Consider using TableRebuilder.Rebuild to rebuild the table and its indexes.");

                // Branch entries always have RecordCount == 0 (see SplitChild),
                // while leaf entries always have RecordCount > 0 (guarded in
                // OnBlockChanged) - but that signal only exists when this
                // index's pointer format actually stores a recordCount field
                // (pointerSize >= 6). For narrower pointer formats (e.g.
                // pointerSize == 2, observed on a real TESTTAB_LARGEDATA.XG0),
                // RecordCount is always 0 regardless of level, so fall back
                // to the block-number-range heuristic in that case.
                bool isLeaf = IsLeafNode(cur);

                PxBlock chainNext = (cur.LeftChildBlockNumber != 0 && IsValidIndexBlockNumber(cur.LeftChildBlockNumber))
                    ? ReadBlock(cur.LeftChildBlockNumber) : null;

                if (isLeaf)
                {
                    for (int i = 0; i < cur.Entries.Count; i++)
                    {
                        var entry     = cur.Entries[i];
                        var nextEntry = i < cur.Entries.Count - 1 ? cur.Entries[i + 1] : null;
                        var indexRec  = BuildSyntheticRecord(entry);
                        ParadoxReader.ParadoxRecord nextRec;
                        if (nextEntry != null)
                            nextRec = BuildSyntheticRecord(nextEntry);
                        else if (chainNext != null && chainNext.Entries.Count > 0)
                            nextRec = BuildSyntheticRecord(chainNext.Entries[0]);
                        else
                            nextRec = upperBoundRec;

                        bool possible = condition.IsIndexPossible(indexRec, nextRec);
                        if (!possible) continue;

                        // Leaf entries store the .DB block number 1-based, but
                        // ParadoxFile.GetBlock indexes blocks 0-based, so convert here.
                        //
                        // A rebuilt/shrunk .DB can leave a secondary index with a
                        // leaf entry pointing at a block number that no longer
                        // exists in the .DB file (observed on real corpus data
                        // after a BDE rebuild). GetBlock has no bounds checking, so
                        // that previously surfaced as an uncaught
                        // EndOfStreamException from deep inside the reader. Skip
                        // stale entries instead of crashing the whole lookup.
                        int dbBlockNumber = entry.BlockNumber - 1;
                        if (dbBlockNumber < 0 || dbBlockNumber >= table.fileBlocks)
                            continue;

                        var block = table.GetBlock((ushort)dbBlockNumber);
                        for (int r = 0; r < block.RecordCount; r++)
                        {
                            var rec = block[r];
                            if (condition.IsDataOk(rec)) yield return rec;
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < cur.Entries.Count; i++)
                    {
                        var entry     = cur.Entries[i];
                        var nextEntry = i < cur.Entries.Count - 1 ? cur.Entries[i + 1] : null;
                        var indexRec  = BuildSyntheticRecord(entry);
                        ParadoxReader.ParadoxRecord nextRec;
                        if (nextEntry != null)
                            nextRec = BuildSyntheticRecord(nextEntry);
                        else if (chainNext != null && chainNext.Entries.Count > 0)
                            nextRec = BuildSyntheticRecord(chainNext.Entries[0]);
                        else
                            nextRec = upperBoundRec;

                        bool possible = condition.IsIndexPossible(indexRec, nextRec);
                        if (!possible) continue;

                        foreach (var rec in EnumerateNode(ReadBlock(entry.BlockNumber), condition, table, nextRec, depth + 1))
                            yield return rec;
                    }
                }


                cur = chainNext;
            }
        }

        /// <summary>
        /// True if <paramref name="blockNumber"/> refers to a block that
        /// physically exists within this index file itself (as opposed to
        /// a .DB block number in the parent table, which can be arbitrarily
        /// larger since it is a different file).
        /// </summary>
        private bool IsValidIndexBlockNumber(ushort blockNumber)
        {
            int blockSize = indexFile.maxTableSize * 0x400;
            long pos = indexFile.headerSize + (long)(blockNumber - blockBase) * blockSize;
            return blockNumber >= blockBase && pos >= indexFile.headerSize && pos + blockSize <= indexFile.stream.Length;
        }

        /// <summary>
        /// Determines whether root block numbers in this index file are 0-based
        /// or 1-based by checking which interpretation makes the file's own
        /// pxRootBlockId resolve to a physically valid in-file block position.
        /// Falls back to <paramref name="typeDefaultBase"/> (the documented
        /// convention for this file's <see cref="ParadoxFileType"/>) if neither
        /// interpretation validates cleanly, or if the file has no records yet
        /// (root block is meaningless for an empty/new index).
        /// </summary>
        private static ushort ResolveBlockBase(ushort typeDefaultBase, ParadoxFile indexFile)
        {
            if (indexFile.RecordCount <= 0 || indexFile.stream.Length <= indexFile.headerSize)
                return typeDefaultBase;

            int blockSize = indexFile.maxTableSize * 0x400;
            bool IsValidUnder(ushort candidateBase)
            {
                long pos = indexFile.headerSize + (long)(indexFile.pxRootBlockId - candidateBase) * blockSize;
                return indexFile.pxRootBlockId >= candidateBase && pos >= indexFile.headerSize && pos + blockSize <= indexFile.stream.Length;
            }

            if (IsValidUnder(typeDefaultBase)) return typeDefaultBase;

            ushort alternateBase = typeDefaultBase == 0 ? (ushort)1 : (ushort)0;
            if (IsValidUnder(alternateBase)) return alternateBase;

            return typeDefaultBase;
        }

        /// <summary>
        /// Builds a synthetic <see cref="ParadoxRecord"/> from a B-tree leaf/
        /// branch entry's key data, so <see cref="ParadoxCondition"/> can
        /// evaluate it via the same DataValues-indexed API used for real
        /// table rows. Values are ordered per this index's composed key
        /// (see <see cref="FieldIndices"/>), not the parent table's layout.
        /// </summary>
        private ParadoxReader.ParadoxRecord BuildSyntheticRecord(PxEntry entry)
        {
            var values = KeySerializer.Deserialize(entry.KeyData, indexedFields);
            return new ParadoxReader.ParadoxRecord(entry.BlockNumber, 0, values);
        }

        /// <summary>
        /// Previously mirrored the parent .DB file's changeCount1/changeCount2 bytes
        /// (offset 0x2D) into this index file. See <see cref="PrimaryIndex.SyncChangeCount"/>
        /// for why this is a no-op; disproven by direct experiment.
        /// </summary>
        public void SyncChangeCount(byte changeCount1, byte changeCount2)
        {
            // Intentionally no-op; see summary above.
        }

        /// <summary>
        /// Mirrors the parent .DB file's autoIncVal (offset 0x49) into this
        /// index file. BDE/Pdxrbld considers the index out of date if this
        /// doesn't match the table's autoIncVal after an AutoInc field is
        /// assigned.
        /// </summary>
        public void SyncAutoIncVal(int autoIncVal)
        {
            indexFile.autoIncVal = autoIncVal;
            indexFile.stream.Position = ParadoxHeaderOffsets.AutoIncVal;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
                w.Write(indexFile.autoIncVal);
        }

        /// <summary>
        /// Mirrors the parent .DB file's V4Hdr changeCount4 (offset 0x70) into
        /// this index file. BDE/Pdxrbld compares this "table version" counter
        /// against the index's own copy to decide whether the index is out
        /// of date.
        /// </summary>
        public void SyncTableVersion(short changeCount4)
        {
            indexFile.stream.Position = ParadoxHeaderOffsets.ChangeCount4;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
                w.Write(changeCount4);
        }

        /// <summary>
        /// Increments the single-byte write counter at offset 0x2C
        /// (pxlib's unknown2Bx2C[1]) once per index write. See
        /// <see cref="PrimaryIndex.IncrementWriteCounter"/> for rationale.
        /// </summary>
        public void IncrementWriteCounter()
        {
            indexFile.stream.Position = ParadoxHeaderOffsets.WriteCounter;
            int current = indexFile.stream.ReadByte();
            if (current < 0) current = 0;
            byte next = (byte)(current + 1);
            indexFile.stream.Position = ParadoxHeaderOffsets.WriteCounter;
            indexFile.stream.WriteByte(next);
        }

        // ----------------------------------------------------------------
        // Field extraction
        // ----------------------------------------------------------------

        private object[] ExtractIndexValues(object[] allValues)
        {
            var result = new object[fieldIndices.Length];
            for (int i = 0; i < fieldIndices.Length; i++)
            {
                int fi = fieldIndices[i];
                result[i] = (allValues != null && fi < allValues.Length)
                    ? allValues[fi] : null;
            }
            return result;
        }

        // ----------------------------------------------------------------
        // Block-number lookup (leaf entries only represent .DB blocks)
        // ----------------------------------------------------------------

        private class FoundEntry
        {
            public PxBlock Block;
            public int Index;
        }

        private FoundEntry FindEntryForBlock(ushort blockNumber)
        {
            if (indexFile.stream.Length <= indexFile.headerSize) return new FoundEntry { Block = null, Index = -1 };
            if (indexFile.RecordCount <= 0) return new FoundEntry { Block = null, Index = -1 };
            return FindEntryForBlockRecursive(ReadBlock(indexFile.pxRootBlockId), blockNumber);
        }

        /// <summary>
        /// Same leaf-vs-branch classification used by <see cref="EnumerateNode"/>.
        /// A naive check of <see cref="PxBlock.IsLeaf"/> (LeftChildBlockNumber == 0)
        /// is NOT sufficient here: a single logical leaf level can span multiple
        /// physical blocks chained via LeftChildBlockNumber pointing to a *sibling*
        /// continuation block (not a child), so that field alone cannot distinguish
        /// "branch node with a first child" from "leaf node chained to its sibling".
        /// Using the wrong classification causes write-path code (FindEntryForBlock,
        /// BTreeInsert/BTreeDelete) to misinterpret a .DB block number (which can be
        /// arbitrarily large) as an in-file index block number, producing an invalid
        /// (negative) stream position and an ArgumentOutOfRangeException
        /// ("Non-negative number required.") from FileStream.set_Position.
        /// </summary>
        private bool IsLeafNode(PxBlock node)
        {
            return node.Entries.Count == 0 || (pointerSize >= 6
                ? node.Entries[0].RecordCount > 0
                : !IsValidIndexBlockNumber(node.Entries[0].BlockNumber));
        }

        private FoundEntry FindEntryForBlockRecursive(PxBlock node, ushort blockNumber)
        {
            // Iterative traversal with a visited-block guard: on corrupt/unusual
            // index files (observed on real corpus data, e.g. !itemfee), the
            // LeftChildBlockNumber/entry chain can loop back on itself. A plain
            // recursive walk over such a cycle recurses without bound and
            // crashes the whole process with a StackOverflowException (which
            // cannot be caught), so track visited block numbers and iterate
            // instead of recursing to guarantee termination.
            var visited = new HashSet<ushort>();
            var stack = new Stack<PxBlock>();
            stack.Push(node);

            while (stack.Count > 0)
            {
                var cur = stack.Pop();
                if (!visited.Add(cur.BlockNumber)) continue;

                if (IsLeafNode(cur))
                {
                    for (int i = 0; i < cur.Entries.Count; i++)
                        if (cur.Entries[i].BlockNumber == blockNumber)
                            return new FoundEntry { Block = cur, Index = i };

                    // Leaf level continuation: LeftChildBlockNumber here is a sibling
                    // chain pointer, not a child - follow it to keep searching this
                    // same logical leaf level (mirrors EnumerateNode's chainNext).
                    if (cur.LeftChildBlockNumber != 0 && IsValidIndexBlockNumber(cur.LeftChildBlockNumber) &&
                        !visited.Contains(cur.LeftChildBlockNumber))
                        stack.Push(ReadBlock(cur.LeftChildBlockNumber));

                    continue;
                }

                if (cur.LeftChildBlockNumber != 0 && IsValidIndexBlockNumber(cur.LeftChildBlockNumber) &&
                    !visited.Contains(cur.LeftChildBlockNumber))
                    stack.Push(ReadBlock(cur.LeftChildBlockNumber));

                foreach (var e in cur.Entries)
                {
                    if (!IsValidIndexBlockNumber(e.BlockNumber) || visited.Contains(e.BlockNumber)) continue;
                    stack.Push(ReadBlock(e.BlockNumber));
                }
            }

            return new FoundEntry { Block = null, Index = -1 };
        }

        private void RemoveBlockEntry(ushort blockNumber)
        {
            var found = FindEntryForBlock(blockNumber);
            if (found.Block == null) return;
            byte[] key = found.Block.Entries[found.Index].KeyData;
            BTreeDelete(key);
        }

        // ----------------------------------------------------------------
        // B-Tree: Insert (used only when a brand-new .DB block needs its
        // own leaf entry; existing-block record-count/key updates are
        // handled directly in OnBlockChanged without tree restructuring)
        // ----------------------------------------------------------------

        private void BTreeInsert(PxEntry entry)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile.BTreeInsert] rootBlock={indexFile.pxRootBlockId}, " +
                $"key={BitConverter.ToString(entry.KeyData)}");

            var root = ReadBlock(indexFile.pxRootBlockId);
            if (root.IsFull(entrySize))
            {
                var newRoot = AllocateBlock();
                newRoot.LeftChildBlockNumber = indexFile.pxRootBlockId;
                SplitChild(newRoot, 0, root);
                InsertNonFull(newRoot, entry);
                WriteBlock(newRoot);
                UpdateRootBlockId(newRoot.BlockNumber);
                UpdateLevelCount((byte)(indexFile.pxLevelCount + 1));
            }
            else
            {
                InsertNonFull(root, entry);
            }
        }

        private void InsertNonFull(PxBlock node, PxEntry entry, int depth = 0)
        {
            if (depth > MAX_TREE_DEPTH)
                throw new InvalidOperationException(
                    $"[SecondaryIndexFile.InsertNonFull] Exceeded max tree depth ({MAX_TREE_DEPTH}) for '{FilePath}'; " +
                    "the index file appears to be corrupt or cyclic. Consider using TableRebuilder.Rebuild to rebuild the table and its indexes.");

            int i = node.Entries.Count - 1;
            if (IsLeafNode(node))
            {
                node.Entries.Add(null);
                while (i >= 0 && CompareKeys(entry.KeyData, node.Entries[i].KeyData) < 0)
                {
                    node.Entries[i + 1] = node.Entries[i];
                    i--;
                }
                node.Entries[i + 1] = entry;
                WriteBlock(node);
            }
            else
            {
                while (i >= 0 && CompareKeys(entry.KeyData, node.Entries[i].KeyData) < 0)
                    i--;
                i++;
                var child = ReadBlock(i < node.Entries.Count ? node.Entries[i].BlockNumber : node.LeftChildBlockNumber);
                if (child.IsFull(entrySize))
                {
                    SplitChild(node, i, child);
                    if (CompareKeys(entry.KeyData, node.Entries[i].KeyData) > 0) i++;
                }
                var target = i < node.Entries.Count ? node.Entries[i].BlockNumber : node.LeftChildBlockNumber;
                InsertNonFull(ReadBlock(target), entry, depth + 1);
            }
        }

        private void SplitChild(PxBlock parent, int childIndex, PxBlock fullChild)
        {
            var newChild = AllocateBlock();
            newChild.LeftChildBlockNumber = fullChild.LeftChildBlockNumber;
            int median        = fullChild.Entries.Count / 2;
            var medianEntry   = fullChild.Entries[median];
            newChild.Entries  = fullChild.Entries.GetRange(
                                    median + 1,
                                    fullChild.Entries.Count - median - 1);
            fullChild.Entries = fullChild.Entries.GetRange(0, median);
            parent.Entries.Insert(childIndex,
                new PxEntry(medianEntry.KeyData, newChild.BlockNumber, 0));
            WriteBlock(fullChild);
            WriteBlock(newChild);
            WriteBlock(parent);
        }

        // ----------------------------------------------------------------
        // B-Tree: Delete (used only when a .DB block's last record is
        // removed, so its leaf entry must be removed entirely)
        // ----------------------------------------------------------------

        private void BTreeDelete(byte[] keyData)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile.BTreeDelete] rootBlock={indexFile.pxRootBlockId}, " +
                $"key={BitConverter.ToString(keyData)}");

            if (indexFile.stream.Length <= indexFile.headerSize)
                return; // Nothing to delete from an empty tree.

            var root = ReadBlock(indexFile.pxRootBlockId);
            DeleteFromNode(root, keyData);
            if (root.Entries.Count == 0 && !IsLeafNode(root))
            {
                UpdateRootBlockId(root.LeftChildBlockNumber);
                FreeBlock(root.BlockNumber);
                if (indexFile.pxLevelCount > 0)
                    UpdateLevelCount((byte)(indexFile.pxLevelCount - 1));
            }
            UpdateRecordCount(indexFile.RecordCount - 1);
        }

        private void DeleteFromNode(PxBlock node, byte[] keyData, int depth = 0)
        {
            if (depth > MAX_TREE_DEPTH)
                throw new InvalidOperationException(
                    $"[SecondaryIndexFile.DeleteFromNode] Exceeded max tree depth ({MAX_TREE_DEPTH}) for '{FilePath}'; " +
                    "the index file appears to be corrupt or cyclic. Consider using TableRebuilder.Rebuild to rebuild the table and its indexes.");

            int i = FindKeyIndex(node, keyData);
            if (i < node.Entries.Count &&
                CompareKeys(keyData, node.Entries[i].KeyData) == 0)
            {
                if (IsLeafNode(node))
                {
                    node.Entries.RemoveAt(i);
                    WriteBlock(node);
                }
                else
                {
                    var pred = GetPredecessor(node, i);
                    node.Entries[i] = pred;
                    WriteBlock(node);
                    DeleteFromNode(ReadBlock(node.Entries[i].BlockNumber), pred.KeyData, depth + 1);
                }
            }
            else if (!IsLeafNode(node))
            {
                var child = ReadBlock(i < node.Entries.Count
                    ? node.Entries[i].BlockNumber
                    : node.LeftChildBlockNumber);
                DeleteFromNode(child, keyData, depth + 1);
            }
        }

        private PxEntry GetPredecessor(PxBlock node, int idx)
        {
            var cur = ReadBlock(node.Entries[idx].BlockNumber);
            int depth = 0;
            while (!IsLeafNode(cur))
            {
                if (++depth > MAX_TREE_DEPTH)
                    throw new InvalidOperationException(
                        $"[SecondaryIndexFile.GetPredecessor] Exceeded max tree depth ({MAX_TREE_DEPTH}) for '{FilePath}'; " +
                        "the index file appears to be corrupt or cyclic. Consider using TableRebuilder.Rebuild to rebuild the table and its indexes.");
                cur = ReadBlock(cur.Entries[cur.Entries.Count - 1].BlockNumber);
            }
            return cur.Entries[cur.Entries.Count - 1];
        }

        // ----------------------------------------------------------------
        // Block I/O
        // ----------------------------------------------------------------

        // NOTE: .Xnn/.Ynn (secondary index) blocks are never encrypted/decrypted
        // here, even when the parent table is password-protected. Paradox/pxlib
        // only cipher .DB data blocks; the encryption key is merely replicated
        // into the index file's header (via the 0xFF00FF00 sentinel + V4 header
        // Encryption2, see ParadoxFile.EncryptionKey) but the index B-tree block
        // contents themselves are always stored in plaintext. This is
        // intentional, not a gap.
        private PxBlock ReadBlock(ushort blockNumber)
        {
            var block = new PxBlock { BlockNumber = blockNumber, Capacity = blockCapacity };
            int  blockSize = indexFile.maxTableSize * 0x400;
            long pos  = indexFile.headerSize + (long)(blockNumber - blockBase) * blockSize;

            // A rebuilt/inconsistent index (or a root-block-base guess that
            // ResolveBlockBase couldn't validate against any records) can
            // otherwise send this position past end-of-file, which previously
            // surfaced deep inside BinaryReader as an opaque
            // EndOfStreamException. Fail fast with a clearer, still-catchable
            // exception instead.
            if (pos < indexFile.headerSize || pos + blockSize > indexFile.stream.Length)
                throw new InvalidOperationException(
                    $"[SecondaryIndexFile.ReadBlock] Block {blockNumber} is out of range for '{FilePath}'. " +
                    $"offset={pos}, blockSize={blockSize}, streamLength={indexFile.stream.Length}, headerSize={indexFile.headerSize}. " +
                    "The index file appears to be corrupt. Consider using TableRebuilder.Rebuild to rebuild the table and its indexes.");

            indexFile.stream.Position = pos;
            using (var r = new BinaryReader(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
            {
                block.LeftChildBlockNumber = r.ReadUInt16();
                r.ReadUInt16(); // reserved
                ushort usedBytes = r.ReadUInt16();

                // usedBytes = (entryCount - 1) * entrySize, per WriteBlock below
                // (0 for both 0 and 1 entries, but any block reached via traversal
                // is referenced by a parent entry, so it always has >= 1 entry).
                //
                // A rebuilt/stale block can leave garbage in usedBytes (observed
                // on real corpus data after a BDE rebuild), which would otherwise
                // compute an entryCount that reads far past this block's actual
                // capacity and crash with an EndOfStreamException. Clamp to what
                // the block can physically hold.
                int entryCount = usedBytes == 0 ? 1 : (usedBytes / entrySize) + 1;
                int maxEntries = Math.Max(blockCapacity / entrySize, 1);
                if (entryCount < 1 || entryCount > maxEntries) entryCount = Math.Min(Math.Max(entryCount, 1), maxEntries);
                for (int i = 0; i < entryCount; i++)
                {
                    byte[] key = r.ReadBytes(keyDataSize);
                    ushort bn = ReadSignFlippedUInt16(r);
                    ushort rc = pointerSize >= 6 ? ReadSignFlippedUInt16(r) : (ushort)0;
                    if (pointerSize >= 6) r.ReadBytes(2); // reserved word
                    else if (pointerSize > 2) r.ReadBytes(pointerSize - 2);
                    block.Entries.Add(new PxEntry(key, bn, rc));
                }
            }
            return block;
        }

        private void WriteBlock(PxBlock block)
        {
            int  blockSize = indexFile.maxTableSize * 0x400;
            long pos       = indexFile.headerSize + (long)(block.BlockNumber - blockBase) * blockSize;
            indexFile.stream.Position = pos;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
            {
                ushort usedBytes = block.Entries.Count <= 1
                    ? (ushort)0
                    : (ushort)((block.Entries.Count - 1) * entrySize);

                w.Write(block.LeftChildBlockNumber);
                w.Write((ushort)0); // reserved
                w.Write(usedBytes);
                foreach (var e in block.Entries)
                {
                    w.Write(e.KeyData);
                    WriteSignFlippedUInt16(w, e.BlockNumber);
                    if (pointerSize >= 6)
                    {
                        WriteSignFlippedUInt16(w, e.RecordCount);
                        WriteSignFlippedUInt16(w, 0); // reserved word, sign-flipped 0 => 0x8000
                    }
                    else if (pointerSize > 2)
                    {
                        w.Write(new byte[pointerSize - 2]);
                    }
                }
                int used = HEADER_SIZE + block.UsedSize;
                int rem = blockSize - used;
                if (rem > 0) w.Write(new byte[rem]);
            }
        }

        private PxBlock AllocateBlock()
        {
            int    blockSize = indexFile.maxTableSize * 0x400;
            // New block number follows the file's block base convention (0-based for XgnFile, 1-based otherwise).
            ushort n         = (ushort)((indexFile.stream.Length - indexFile.headerSize) / blockSize + blockBase);
            indexFile.stream.SetLength(indexFile.stream.Length + blockSize);
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile.AllocateBlock] Allocated block {n} at " +
                $"offset={indexFile.headerSize + (long)(n - blockBase) * blockSize}, newStreamLength={indexFile.stream.Length}");

            // Keep the index file's own block-chain bookkeeping fields (nextBlock,
            // fileBlocks, firstBlock, lastBlock @ 0xA-0x11) in sync with the
            // actual number of allocated blocks. BDE/Pdxrbld considers the index
            // out of date/corrupt if these aren't updated.
            ushort blockCountFromBase = (ushort)(n - blockBase + 1);
            if (blockCountFromBase == 1) indexFile.firstBlock = n;
            indexFile.nextBlock  = n;
            indexFile.lastBlock  = n;
            indexFile.fileBlocks = blockCountFromBase;
            indexFile.stream.Position = ParadoxHeaderOffsets.BlockChain;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
            {
                w.Write(indexFile.nextBlock);
                w.Write(indexFile.fileBlocks);
                w.Write(indexFile.firstBlock);
                w.Write(indexFile.lastBlock);
            }

            // maxBlocks (word @ 0x3A) tracks allocated capacity; BDE/Pdxrbld flags
            // the index as corrupt if the actual block count ever exceeds it.
            if (blockCountFromBase > indexFile.maxBlocks)
            {
                indexFile.maxBlocks = blockCountFromBase;
                indexFile.stream.Position = ParadoxHeaderOffsets.MaxBlocks;
                using (var w = new BinaryWriter(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
                    w.Write(indexFile.maxBlocks);
            }

            return new PxBlock { BlockNumber = n, Capacity = blockCapacity };
        }

        private void FreeBlock(ushort blockNumber)
        {
            int  blockSize = indexFile.maxTableSize * 0x400;
            long pos       = indexFile.headerSize + (long)(blockNumber - blockBase) * blockSize;
            System.Diagnostics.Debug.WriteLine(
                $"[SecondaryIndexFile.FreeBlock] Zeroing block {blockNumber} at offset={pos}");
            indexFile.stream.Position = pos;
            indexFile.stream.Write(new byte[blockSize], 0, blockSize);
        }

        private void UpdateRootBlockId(ushort newRootId)
        {
            indexFile.pxRootBlockId = newRootId;
            indexFile.stream.Position = ParadoxHeaderOffsets.PxRootBlockId;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
                w.Write(newRootId);
        }

        /// <summary>
        /// Keeps numIndexLevels (@ 0x20) in sync with the actual B-tree depth.
        /// BDE/Pdxrbld considers the index out of date/corrupt if this isn't
        /// updated when the tree grows or shrinks.
        /// </summary>
        private void UpdateLevelCount(byte levelCount)
        {
            indexFile.pxLevelCount = levelCount;
            indexFile.stream.Position = ParadoxHeaderOffsets.PxLevelCount;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
                w.Write(indexFile.pxLevelCount);
        }

        /// <summary>
        /// Keeps this index file's own RecordCount header field (@ 0x06, int32)
        /// in sync with the number of leaf entries (one per .DB block) stored
        /// in the index. BDE/Pdxrbld considers the index out of date/corrupt
        /// if this doesn't match the actual number of leaf entries.
        /// </summary>
        private void UpdateRecordCount(int recordCount)
        {
            indexFile.RecordCount = recordCount;
            indexFile.stream.Position = ParadoxHeaderOffsets.RecordCount;
            using (var w = new BinaryWriter(new NonClosingStreamWrapper(indexFile.stream), Encoding.Default))
                w.Write(indexFile.RecordCount);
        }

        // ----------------------------------------------------------------
        // Key helpers
        // ----------------------------------------------------------------

        private int CompareKeys(byte[] a, byte[] b)
            => KeySerializer.Compare(a, b, indexedFields);

        private int FindKeyIndex(PxBlock node, byte[] keyData)
        {
            int i = 0;
            while (i < node.Entries.Count &&
                   CompareKeys(keyData, node.Entries[i].KeyData) > 0) i++;
            return i;
        }

        private bool IsEmptyKey(byte[] key)
        {
            foreach (var b in key) if (b != 0) return false;
            return true;
        }

        private static ushort ReadSignFlippedUInt16(BinaryReader r)
        {
            byte hi = r.ReadByte();
            byte lo = r.ReadByte();
            ushort encoded = (ushort)((hi << 8) | lo);
            return PxEntry.FlipWord(encoded);
        }

        private static void WriteSignFlippedUInt16(BinaryWriter w, ushort value)
        {
            ushort encoded = PxEntry.FlipWord(value);
            w.Write((byte)(encoded >> 8));
            w.Write((byte)(encoded & 0xFF));
        }

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public void Dispose() => indexFile?.Dispose();
    }
}
