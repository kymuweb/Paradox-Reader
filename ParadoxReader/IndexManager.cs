using System;
using System.Collections.Generic;
using System.IO;

namespace ParadoxReader
{
    /// <summary>
    /// Coordinates updates across all index files (.PX, .Xnn, .Xgn)
    /// associated with a .DB table. Acts as the single point of contact
    /// for all index maintenance during insert, update, and delete operations.
    /// </summary>
    internal class IndexManager : IDisposable
    {
        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly PrimaryIndexFile         PrimaryIndexFile;
        private readonly List<SecondaryIndexFile> secondaryIndexes = new List<SecondaryIndexFile>();
        private readonly ParadoxFile.FieldInfo[]          allFields;
        private readonly int                  primaryKeyFieldCount;

        /// <summary>
        /// All secondary (.Xnn/.Xgn/.Ynn/.Ygn) index files discovered and
        /// opened for the parent table, in discovery order.
        /// </summary>
        internal List<SecondaryIndexFile> SecondaryIndexes => secondaryIndexes;

        /// <summary>
        /// True if any opened index file (.PX/.Xnn/.Xgn/.Ynn/.Ygn) was
        /// found to be out of date relative to the parent .DB (its
        /// autoIncVal doesn't match) when the table was opened. Callers can
        /// check this without triggering an exception; attempting to
        /// actually read/write via an out-of-date index still throws
        /// <see cref="IndexOutOfDateException"/>.
        /// </summary>
        public bool IndexOutOfDate { get; private set; }

        /// <summary>
        /// True specifically if the primary (.PX) index is out of date.
        /// Used by <see cref="ParadoxTableFile"/> to flag its separate
        /// read-side <see cref="ParadoxPrimaryKey"/> handle.
        /// </summary>
        public bool IsPrimaryIndexOutOfDate { get; private set; }

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public IndexManager(string dbFilePath, ParadoxFile.FieldInfo[] allFields, int primaryKeyFieldCount, int dbAutoIncVal)
        {
            this.allFields            = allFields;
            this.primaryKeyFieldCount = primaryKeyFieldCount;

            // Primary index (.PX)
            if (primaryKeyFieldCount > 0)
            {
                string pxPath = Path.ChangeExtension(dbFilePath, ".PX");
                if (File.Exists(pxPath))
                {
                    var keyFields = GetFieldRange(0, primaryKeyFieldCount);
                    PrimaryIndexFile  = new PrimaryIndexFile(pxPath, keyFields);
                    if (IsIndexOutOfDate(PrimaryIndexFile.AutoIncVal, dbAutoIncVal))
                    {
                        PrimaryIndexFile.MarkOutOfDate();
                        IndexOutOfDate = true;
                        IsPrimaryIndexOutOfDate = true;
                    }
                }
            }

            // Secondary indexes (.Xnn, .Xgn)
            DiscoverAndOpenSecondaryIndexes(dbFilePath, dbAutoIncVal);
        }

        /// <summary>
        /// Compares an index file's own autoIncVal header field (offset
        /// 0x49) against the parent .DB's autoIncVal. BDE/Pdxrbld considers
        /// an index out of date if this doesn't match after an AutoInc
        /// field is assigned (see PrimaryIndexFile.SyncAutoIncVal); a
        /// nonzero mismatch means the index predates (or postdates) the
        /// current data and must be treated as stale, mirroring BDE's
        /// "Index is out of date" error rather than silently returning
        /// wrong/empty lookup results or corrupting the index further on
        /// write.
        /// </summary>
        private static bool IsIndexOutOfDate(int indexAutoIncVal, int dbAutoIncVal)
        {
            return dbAutoIncVal != 0 && indexAutoIncVal != dbAutoIncVal;
        }


        // ----------------------------------------------------------------
        // Public API
        // ----------------------------------------------------------------

        /// <summary>
        /// Notifies all index files that a .DB data block's contents have
        /// changed (a record was inserted, updated, or deleted within it).
        /// Paradox leaf index entries are per-DB-BLOCK, not per-row: each
        /// leaf entry stores the key of the block's first row plus the
        /// block's current record count. So the only information index
        /// maintenance ever needs is the block's current first-row values
        /// and its current record count; this must be called after every
        /// insert/update/delete that changes a block's contents.
        /// </summary>
        /// <param name="firstRowFieldValues">
        /// All field values of the record now at index 0 in the block, or
        /// null if the block is now empty (recordCount == 0).
        /// </param>
        /// <param name="blockNumber">The affected .DB block number.</param>
        /// <param name="recordCount">The block's current record count.</param>
        public void OnBlockChanged(object[] firstRowFieldValues, ushort blockNumber, int recordCount)
        {
            PrimaryIndexFile?.OnBlockChanged(
                GetKeyValues(firstRowFieldValues, 0, primaryKeyFieldCount),
                blockNumber, recordCount);

            foreach (var idx in secondaryIndexes)
                idx.OnBlockChanged(firstRowFieldValues, blockNumber, recordCount);
        }

        /// <summary>
        /// Mirrors the parent .DB file's changeCount1/changeCount2 bytes into every
        /// open index file, so BDE does not consider the indexes out of date after
        /// a write. Must be called after every insert/update/delete, once the .DB
        /// header's change count has been incremented.
        /// </summary>
        public void SyncChangeCount(byte changeCount1, byte changeCount2)
        {
            PrimaryIndexFile?.SyncChangeCount(changeCount1, changeCount2);

            foreach (var idx in secondaryIndexes)
                idx.SyncChangeCount(changeCount1, changeCount2);
        }

        /// <summary>
        /// Mirrors the parent .DB file's autoIncVal (offset 0x49) into every
        /// open index file. BDE/Pdxrbld considers an index out of date if its
        /// own autoIncVal doesn't match the table's after an AutoInc field is
        /// assigned.
        /// </summary>
        public void SyncAutoIncVal(int autoIncVal)
        {
            PrimaryIndexFile?.SyncAutoIncVal(autoIncVal);

            foreach (var idx in secondaryIndexes)
                idx.SyncAutoIncVal(autoIncVal);
        }

        /// <summary>
        /// Mirrors the parent .DB file's V4Hdr changeCount4 (offset 0x70) into
        /// every open index file. BDE/Pdxrbld compares this "table version"
        /// counter against each index's own copy to decide whether the index
        /// is out of date.
        /// </summary>
        public void SyncTableVersion(short changeCount4)
        {
            PrimaryIndexFile?.SyncTableVersion(changeCount4);

            foreach (var idx in secondaryIndexes)
                idx.SyncTableVersion(changeCount4);
        }

        /// <summary>
        /// Increments the single-byte write counter at offset 0x2C in every
        /// open index file. Discovered via multi-pass SQLRunner testing:
        /// this byte increments by 1 on every write to a valid .PX/.Xnn
        /// file, independently of the .DB file's own changeCount. Being
        /// tried as the true "index version" field.
        /// </summary>
        public void IncrementWriteCounter()
        {
            PrimaryIndexFile?.IncrementWriteCounter();

            foreach (var idx in secondaryIndexes)
                idx.IncrementWriteCounter();
        }

        // ----------------------------------------------------------------
        // Secondary index discovery
        // ----------------------------------------------------------------

        private void DiscoverAndOpenSecondaryIndexes(string dbFilePath, int dbAutoIncVal)
        {
            var discovered = SecondaryIndexDiscovery.Discover(dbFilePath, allFields, primaryKeyFieldCount);
            foreach (var info in discovered)
            {
                try
                {
                    var secondaryIndex = new SecondaryIndexFile(info.FilePath, info.IndexedFields, info.FieldIndices);
                    if (IsIndexOutOfDate(secondaryIndex.AutoIncVal, dbAutoIncVal))
                    {
                        secondaryIndex.MarkOutOfDate();
                        IndexOutOfDate = true;
                    }
                    secondaryIndexes.Add(secondaryIndex);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine(
                        $"[IndexManager] Failed to open {info.FilePath}: {ex.Message}");
                }
            }
        }

        // ----------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------

        private ParadoxFile.FieldInfo[] GetFieldRange(int start, int count)
        {
            var fields = new ParadoxFile.FieldInfo[count];
            Array.Copy(allFields, start, fields, 0, count);
            return fields;
        }

        private object[] GetKeyValues(object[] fieldValues, int start, int count)
        {
            if (count <= 0 || fieldValues == null) return new object[0];
            var keys = new object[count];
            Array.Copy(fieldValues, start, keys, 0,
                       Math.Min(count, fieldValues.Length - start));
            return keys;
        }

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public void Dispose()
        {
            PrimaryIndexFile?.Dispose();
            foreach (var idx in secondaryIndexes)
                idx.Dispose();
        }
    }
}