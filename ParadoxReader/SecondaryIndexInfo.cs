namespace ParadoxReader
{
    /// <summary>
    /// Describes a discovered secondary index and its field mapping
    /// relative to the parent .DB table.
    /// </summary>
    internal class SecondaryIndexInfo
    {
        /// <summary>Full path to the .Xnn or .Xgn file.</summary>
        public string FilePath { get; set; }

        /// <summary>FieldInfo entries that this index covers.</summary>
        public ParadoxFile.FieldInfo[] IndexedFields { get; set; }

        /// <summary>
        /// 0-based field positions in the parent .DB table
        /// corresponding to IndexedFields.
        /// </summary>
        public int[] FieldIndices { get; set; }
    }
}
