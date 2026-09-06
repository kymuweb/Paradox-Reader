using System;
using System.IO;

namespace ParadoxReader
{
    public class ParadoxRecord
    {
        // ----------------------------------------------------------------
        // Properties
        // ----------------------------------------------------------------

        /// <summary>Block number in the .DB file (1-based).</summary>
        public ushort BlockNumber { get; internal set; }

        /// <summary>Record index within the block (0-based).</summary>
        public ushort RecordIndex { get; internal set; }

        /// <summary>Field values. Use CloneDataValues() to get a safe copy before modifying.</summary>
        public object[] DataValues { get; set; }

        // Kept so Save() can write back to the correct block and update indexes
        private readonly ParadoxFile.DataBlock block;

        // Snapshot of DataValues at the time the record was read — needed as
        // the "old" key values when calling IndexManager.OnUpdate.
        private readonly object[] originalDataValues;

        // ----------------------------------------------------------------
        // Constructor used by ParadoxDb (new write path — no block ref needed)
        // ----------------------------------------------------------------

        public ParadoxRecord(ushort blockNumber, ushort recordIndex, object[] dataValues)
        {
            BlockNumber = blockNumber;
            RecordIndex = recordIndex;
            DataValues  = dataValues;
        }

        // ----------------------------------------------------------------
        // Constructor used by existing DataBlock indexer (read path)
        // Reads field values from already-loaded block.data in memory.
        // ----------------------------------------------------------------

        internal ParadoxRecord(ParadoxFile.DataBlock block, int recIndex)
        {
            this.block         = block;
            BlockNumber        = block.blockNumber;
            RecordIndex        = (ushort)recIndex;
            DataValues         = ParseDataValues(block, recIndex);
            originalDataValues = (object[])DataValues.Clone(); // shallow copy sufficient: values are immutable scalars or byte[]
        }

        // ----------------------------------------------------------------
        // Save — serialize DataValues back to block.data and write to disk
        // ----------------------------------------------------------------

        /// <summary>
        /// Serializes the current DataValues back to the underlying block
        /// and writes that block to the .DB file on disk.
        /// Only valid for records obtained via table.Enumerate().
        /// </summary>
        public void Save()
        {
            if (block == null)
                throw new InvalidOperationException(
                    "Save() is only available on records read via table.Enumerate(). " +
                    "Use ParadoxDb.UpdateRecord() for records created manually.");

            var file       = block.file;
            var fieldTypes = file.FieldTypes;
            int offset     = RecordIndex * file.RecordSize;

            // Serialize DataValues back into block.data at the correct offset
            using (var ms = new MemoryStream(block.data, offset, file.RecordSize, writable: true))
            using (var w  = new BinaryWriter(ms))
            {
                for (int i = 0; i < fieldTypes.Length; i++)
                {
                    var field = fieldTypes[i];
                    var value = (DataValues != null && i < DataValues.Length)
                        ? DataValues[i] : null;
                    WriteField(w, file, field, value);
                }
            }

            // Write the updated block back to the .DB file
            block.WriteRecordToFile(RecordIndex);

            // Keep index files (.PX, .Xnn, .Xgn) in sync
            UpdateIndexes(file);
        }

        private void UpdateIndexes(ParadoxFile file)
        {
            // We can only derive the file path when the underlying stream is a FileStream
            var fs = file.stream as System.IO.FileStream;
            if (fs == null) return;

            string dbPath = fs.Name;
            int pkCount = file.primaryKeyFields;

            try
            {
                using (var idxMgr = new IndexManager(dbPath, file.FieldTypes, pkCount, file.autoIncVal))
                {
                    // Paradox leaf index entries are per-DB-BLOCK, not per-row (see
                    // PrimaryIndexFile/SecondaryIndexFile remarks): index maintenance only
                    // needs the block's current first-row field values and record
                    // count, not the specific row that changed.
                    object[] firstRowValues = block.RecordCount > 0 ? block[0].DataValues : null;
                    idxMgr.OnBlockChanged(firstRowValues, BlockNumber, block.RecordCount);
                }
            }
            catch (Exception ex)
            {
                // Surface index errors clearly rather than silently swallowing them
                throw new InvalidOperationException(
                    $"[ParadoxRecord.Save] Index update failed: {ex.Message}", ex);
            }
        }

        // ----------------------------------------------------------------
        // Parse field values from block.data (in-memory read)
        // ----------------------------------------------------------------

        private static object[] ParseDataValues(ParadoxFile.DataBlock block, int recIndex)
        {
            var file       = block.file;
            var fieldTypes = file.FieldTypes;
            int offset     = recIndex * file.RecordSize;

            using (var ms = new MemoryStream(block.data, offset, file.RecordSize, writable: false))
            using (var r  = new BinaryReader(ms))
            {
                var values = new object[fieldTypes.Length];
                for (int i = 0; i < fieldTypes.Length; i++)
                    values[i] = ReadField(r, file, fieldTypes[i]);
                return values;
            }
        }

        /// <summary>
        /// Deserializes raw record bytes (as stored in a .DB data block) back
        /// into field values, using the same per-field decoding as the
        /// in-memory read path. Used by <see cref="ParadoxTableFile"/> to
        /// determine a block's first-row key values after an insert/update/
        /// delete, which Paradox's per-DB-block leaf index entries require.
        /// </summary>
        internal static object[] DeserializeRecordBytes(byte[] recordBytes, ParadoxFile file)
        {
            var fieldTypes = file.FieldTypes;
            using (var ms = new MemoryStream(recordBytes, 0, recordBytes.Length, writable: false))
            using (var r  = new BinaryReader(ms))
            {
                var values = new object[fieldTypes.Length];
                for (int i = 0; i < fieldTypes.Length; i++)
                    values[i] = ReadField(r, file, fieldTypes[i]);
                return values;
            }
        }

        // ----------------------------------------------------------------
        // Read a single field value
        // ----------------------------------------------------------------

        private static object ReadField(BinaryReader r, ParadoxFile file, ParadoxFile.FieldInfo field)
        {
            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                    return r.ReadPdoxString(field.fSize);

                case ParadoxFieldTypes.Short:
                    return r.ReadPdoxShort(field.fSize);

                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                    return r.ReadPdoxInt(field.fSize);

                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    return r.ReadPdoxDouble(field.fSize);

                case ParadoxFieldTypes.Date:
                    return r.ReadPdoxDate(field.fSize);

                case ParadoxFieldTypes.Time:
                    return r.ReadPdoxTime(field.fSize);

                case ParadoxFieldTypes.Timestamp:
                    return r.ReadPdoxTimestamp(field.fSize);

                case ParadoxFieldTypes.Logical:
                    return r.ReadPdoxBool(field.fSize);

                case ParadoxFieldTypes.BCD:
                    return r.ReadPdoxBCD(bCDDecLen: field.fSize, bCDDataLen: 17);

                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                {
                    var blobInfo = r.ReadBytes(field.fSize);
                    // hsize=9: on-disk type-2 block header is type(1)+numBlocks(2)+
                    // blobLen(4)+modNr(2)=9 bytes (pxlib's _TMbBlockHeader2). Type-3
                    // sub-blocks ignore hsize entirely, so this only affects type-2.
                    var blob     = file.ReadBlob(blobInfo, field.fSize, 9);
                    // Always return a MemoValue carrying blobInfo, even when the memo is
                    // blank/never-written (blob == null, blobInfo all zero). Returning null
                    // here loses blobInfo entirely, which then prevents WriteField from ever
                    // calling WriteBlob on save (it falls back to writing zero bytes and the
                    // memo is silently never persisted).
                    var text = blob == null ? string.Empty : System.Text.Encoding.Default.GetString(blob);
                    return new MemoValue(text, blobInfo);
                }

                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                {
                    // Return the raw reference bytes from the .DB record (the blob pointer).
                    // These must survive the read → modify → write cycle unchanged so the
                    // .MB entry they point to is never orphaned.
                    // Use file.ReadBlob(blobInfo, ...) separately if you need the content.
                    var blobInfo = r.ReadBytes(field.fSize);
                    return blobInfo;
                }

                case ParadoxFieldTypes.Bytes:
                    return r.ReadPdoxBytes(field.fSize);

                default:
                    r.ReadBytes(field.fSize);
                    return null;
            }
        }

        // ----------------------------------------------------------------
        // Write a single field value
        // ----------------------------------------------------------------

        private static void WriteField(BinaryWriter w, ParadoxFile file,
                                       ParadoxFile.FieldInfo field, object value)
        {
            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                    w.WritePdoxString(value as string ?? string.Empty, field.fSize);
                    break;

                case ParadoxFieldTypes.Short:
                    w.WritePdoxShort(value is short s ? s : Convert.ToInt16(value ?? 0), field.fSize);
                    break;

                case ParadoxFieldTypes.AutoInc:
                    // AutoInc is managed by Paradox — skip writing any user-supplied value
                    // by advancing the writer position without changing the underlying bytes.
                    w.BaseStream.Seek(field.fSize, SeekOrigin.Current);
                    break;

                case ParadoxFieldTypes.Long:
                    w.WritePdoxInt(value is int i ? i : Convert.ToInt32(value ?? 0), field.fSize);
                    break;

                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    w.WritePdoxDouble(value is double d ? d : Convert.ToDouble(value ?? 0.0), field.fSize);
                    break;

                case ParadoxFieldTypes.Date:
                    w.WritePdoxDate(value is DateTime dt ? dt : Convert.ToDateTime(value ?? DateTime.MinValue), field.fSize);
                    break;

                case ParadoxFieldTypes.Time:
                    w.WritePdoxTime(value is TimeSpan ts ? ts
                                  : value is DateTime dtv ? dtv.TimeOfDay
                                  : TimeSpan.Zero, field.fSize);
                    break;

                case ParadoxFieldTypes.Timestamp:
                    w.WritePdoxTimestamp(value is DateTime dts ? dts : Convert.ToDateTime(value ?? DateTime.MinValue), field.fSize);
                    break;

                case ParadoxFieldTypes.Logical:
                    w.WritePdoxBool(value is bool b ? b : Convert.ToBoolean(value ?? false), field.fSize);
                    break;

                case ParadoxFieldTypes.BCD:
                    w.WritePdoxBCD(value is decimal dec ? dec : Convert.ToDecimal(value ?? 0m),
                                   bCDDecLen: field.fSize, bCDDataLen: 17);
                    break;

                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                {
                    if (value is MemoValue mv && mv.BlobInfo != null)
                    {
                        // Encode the new text and overwrite the slot in the .MB file.
                        // hsize=9: matches ReadField above (on-disk type-2 header size).
                        byte[] encoded = System.Text.Encoding.Default.GetBytes(mv.Text ?? string.Empty);
                        file.WriteBlob(mv.BlobInfo, field.fSize, 9, encoded);

                        int leader = field.fSize - 10;

                        // The first `leader` bytes of blobInfo are an inline prefix of the
                        // text content stored directly in the .DB record.  Update them so
                        // the .DB record reflects the new text value.
                        if (leader > 0)
                        {
                            int copyLen = Math.Min(leader, encoded.Length);
                            Array.Copy(encoded, 0, mv.BlobInfo, 0, copyLen);
                            // Zero any remaining leader bytes when the new text is shorter.
                            for (int li = copyLen; li < leader; li++)
                                mv.BlobInfo[li] = 0;
                        }

                        // Note: WriteBlob has already updated blobInfo[leader+8] (mod_nr)
                        // to the new global slot sequence value — no separate increment needed.
                        w.WritePdoxBytes(mv.BlobInfo, field.fSize);
                    }
                    else
                    {
                        // Fallback: write whatever reference bytes we have (or zeros).
                        byte[] blobRef = value as byte[] ?? new byte[field.fSize];
                        w.WritePdoxBytes(blobRef, field.fSize);
                    }
                    break;
                }

                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                {
                    byte[] blobRef = value as byte[] ?? new byte[field.fSize];
                    w.WritePdoxBytes(blobRef, field.fSize);
                    break;
                }

                case ParadoxFieldTypes.Bytes:
                    w.WritePdoxBytes(value as byte[] ?? new byte[field.fSize], field.fSize);
                    break;

                default:
                    w.Write(new byte[field.fSize]);
                    break;
            }
        }

        // ----------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------

        /// <summary>Gets a field value by 0-based index.</summary>
        public object this[int fieldIndex]
            => (DataValues != null && fieldIndex < DataValues.Length)
               ? DataValues[fieldIndex] : null;

        /// <summary>
        /// Returns a deep copy of DataValues so callers cannot accidentally
        /// mutate the cached record values.
        /// </summary>
        public object[] CloneDataValues()
        {
            if (DataValues == null) return null;
            var copy = new object[DataValues.Length];
            for (int i = 0; i < DataValues.Length; i++)
            {
                copy[i] = DataValues[i] is byte[] b
                    ? (object)((byte[])b.Clone())
                    : DataValues[i];
            }
            return copy;
        }
    }
}
