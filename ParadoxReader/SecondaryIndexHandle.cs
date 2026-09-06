using System.Collections.Generic;

namespace ParadoxReader
{
    /// <summary>
    /// Public read-side handle for a single secondary index (.Xnn/.Xgn/.Ynn/.Ygn)
    /// file, exposed via <see cref="ParadoxTableFile.SecondaryIndexes"/>. Wraps
    /// the internal <see cref="SecondaryIndexFile"/> B-tree so callers outside
    /// this assembly can perform condition-based index lookups, mirroring
    /// <see cref="ParadoxPrimaryKey.Enumerate(ParadoxCondition)"/> for the
    /// primary (.PX) index.
    /// </summary>
    public class SecondaryIndexHandle
    {
        private readonly SecondaryIndexFile indexFile;
        private readonly ParadoxFile         table;

        internal SecondaryIndexHandle(SecondaryIndexFile indexFile, ParadoxFile table)
        {
            this.indexFile = indexFile;
            this.table     = table;
        }

        /// <summary>Full path to the underlying index file.</summary>
        public string FilePath => indexFile.FilePath;

        /// <summary>
        /// 0-based field positions in the parent table that this index's
        /// composed key covers, in key order (indexed field(s) followed by
        /// appended primary-key field(s)). When constructing a
        /// <see cref="ParadoxCondition.Compare"/> for use with
        /// <see cref="Enumerate"/>, its IndexFieldIndex must refer to the
        /// position of the target field within this array, not its position
        /// in the parent table.
        /// </summary>
        public int[] FieldIndices => indexFile.FieldIndices;

        /// <summary>
        /// Traverses this secondary index's B-tree, using
        /// <paramref name="condition"/> to prune branches (IsIndexPossible)
        /// and filter matching rows (IsDataOk), and returns the matching
        /// records from the parent .DB table.
        /// </summary>
        public IEnumerable<ParadoxRecord> Enumerate(ParadoxCondition condition)
            => indexFile.Enumerate(condition, table);
    }
}
