using System;

namespace ParadoxReader
{
    /// <summary>
    /// Thrown when a table's primary (.PX) or secondary (.Xnn/.Xgn) index
    /// file is detected to be out of date relative to the parent .DB file
    /// (e.g. the index's own RecordCount header field doesn't match the
    /// .DB's), mirroring BDE's "Index is out of date" error. Recovery is
    /// normally to rebuild the indexes, e.g. via <see cref="TableRebuilder.Rebuild(string, string)"/>.
    /// </summary>
    public class IndexOutOfDateException : Exception
    {
        public string IndexFilePath { get; }

        public IndexOutOfDateException(string indexFilePath, string message)
            : base(message)
        {
            IndexFilePath = indexFilePath;
        }
    }
}
