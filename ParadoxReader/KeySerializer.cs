using System;
using System.IO;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Serializes Paradox field values to raw bytes and compares
    /// serialized key byte arrays field by field.
    /// Matches the binary encoding used in .DB, .PX, and .Xnn files.
    /// </summary>
    internal static class KeySerializer
    {
        // ----------------------------------------------------------------
        // Serialize
        // ----------------------------------------------------------------

        public static byte[] Serialize(object[] fieldValues, ParadoxFile.FieldInfo[] fields)
            => Serialize(fieldValues, fields, null);

        /// <summary>
        /// Serializes field values to raw record bytes. When <paramref name="file"/> is
        /// provided, MemoBLOb/FmtMemoBLOb fields carrying a <see cref="MemoValue"/> will
        /// have their text persisted to the associated .MB blob file via
        /// <see cref="ParadoxFile.WriteBlob"/>; without a file, only the raw blobInfo
        /// reference bytes are written (used by index serialization, which never
        /// includes memo fields).
        /// </summary>
        public static byte[] Serialize(object[] fieldValues, ParadoxFile.FieldInfo[] fields, ParadoxFile file)
        {
            using (var ms = new MemoryStream())
            using (var w  = new BinaryWriter(ms))
            {
                for (int i = 0; i < fields.Length; i++)
                {
                    object value = (fieldValues != null && i < fieldValues.Length)
                        ? fieldValues[i] : null;
                    WriteField(w, value, fields[i], file);
                }
                return ms.ToArray();
            }
        }

        private static void WriteField(BinaryWriter w, object value, ParadoxFile.FieldInfo field, ParadoxFile file = null)
        {
            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                {
                    string str   = value as string ?? "";
                    byte[] bytes = Encoding.Default.GetBytes(str);
                    Array.Resize(ref bytes, field.fSize);
                    w.Write(bytes);
                    break;
                }

                case ParadoxFieldTypes.Short:
                {
                    short v = value == null ? (short)0
                            : value is short s ? s
                            : Convert.ToInt16(value);
                    ushort encoded = (ushort)(v ^ unchecked((short)0x8000));
                    w.Write((byte)(encoded >> 8));
                    w.Write((byte)(encoded & 0xFF));
                    break;
                }

                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                {
                    int  v       = value == null ? 0
                                 : value is int i ? i
                                 : Convert.ToInt32(value);
                    uint encoded = (uint)(v ^ unchecked((int)0x80000000));
                    WriteBigEndianUInt32(w, encoded);
                    break;
                }

                case ParadoxFieldTypes.Date:
                {
                    int v = value == null          ? 0
                          : value is int i         ? i
                          : value is DateTime dt   ? DateTimeToParadoxDate(dt)
                          : Convert.ToInt32(value);
                    WriteBigEndianUInt32(w, (uint)(v ^ unchecked((int)0x80000000)));
                    break;
                }

                case ParadoxFieldTypes.Time:
                {
                    int v = value == null          ? 0
                          : value is int i         ? i
                          : value is TimeSpan ts   ? (int)ts.TotalMilliseconds
                          : Convert.ToInt32(value);
                    WriteBigEndianUInt32(w, (uint)(v ^ unchecked((int)0x80000000)));
                    break;
                }

                case ParadoxFieldTypes.Timestamp:
                {
                    double v = value == null        ? 0.0
                             : value is double d    ? d
                             : value is DateTime dt ? DateTimeToParadoxTimestamp(dt)
                             : Convert.ToDouble(value);
                    byte[] bytes = BitConverter.GetBytes(v);
                    BinaryReaderWriterPdoxExtensions.ConvertBytesForDouble(bytes, 8, inverse: true);
                    w.Write(bytes);
                    break;
                }

                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                {
                    double v = value == null     ? 0.0
                             : value is double d ? d
                             : Convert.ToDouble(value);
                    byte[] bytes = BitConverter.GetBytes(v);
                    BinaryReaderWriterPdoxExtensions.ConvertBytesForDouble(bytes, 8, inverse: true);
                    w.Write(bytes);
                    break;
                }

                case ParadoxFieldTypes.Logical:
                {
                    bool b = value != null && Convert.ToBoolean(value);
                    w.Write(b ? (byte)0x81 : (byte)0x80);
                    break;
                }

                case ParadoxFieldTypes.BCD:
                {
                    decimal v = value == null          ? 0m
                              : value is decimal dec   ? dec
                              : Convert.ToDecimal(value);
                    w.WritePdoxBCD(v, bCDDecLen: field.fSize, bCDDataLen: 17);
                    break;
                }

                case ParadoxFieldTypes.Bytes:
                {
                    byte[] bytes = value as byte[] ?? new byte[field.fSize];
                    if (bytes.Length != field.fSize)
                        Array.Resize(ref bytes, field.fSize);
                    w.Write(bytes);
                    break;
                }

                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                {
                    if (file != null && value is MemoValue mv && mv.BlobInfo != null)
                    {
                        // Encode the new text and overwrite the slot in the .MB file.
                        // hsize=9: matches the on-disk type-2 header size (see ParadoxRecord.WriteField).
                        byte[] encoded = System.Text.Encoding.Default.GetBytes(mv.Text ?? string.Empty);
                        file.WriteBlob(mv.BlobInfo, field.fSize, 9, encoded);

                        int leader = field.fSize - 10;

                        // The first `leader` bytes of blobInfo are an inline prefix of the
                        // text content stored directly in the .DB record. Update them so
                        // the .DB record reflects the new text value.
                        if (leader > 0)
                        {
                            int copyLen = Math.Min(leader, encoded.Length);
                            Array.Copy(encoded, 0, mv.BlobInfo, 0, copyLen);
                            for (int li = copyLen; li < leader; li++)
                                mv.BlobInfo[li] = 0;
                        }

                        // WriteBlob has already updated blobInfo[leader+8] (mod_nr) to the
                        // new global slot sequence value — no separate increment needed.
                        byte[] refBytes = mv.BlobInfo;
                        if (refBytes.Length != field.fSize)
                            Array.Resize(ref refBytes, field.fSize);
                        w.Write(refBytes);
                        break;
                    }

                    // No file/MemoValue available (e.g. index key serialization): fall
                    // back to writing whatever raw reference bytes we have (or zeros).
                    goto case ParadoxFieldTypes.BLOb;
                }

                // BLOB types: stored as an offset/length reference, not inline data
                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                {
                    byte[] blobRef = value as byte[] ?? new byte[field.fSize];
                    if (blobRef.Length != field.fSize)
                        Array.Resize(ref blobRef, field.fSize);
                    w.Write(blobRef);
                    break;
                }

                default:
                    w.Write(new byte[field.fSize]);
                    break;
            }
        }

        // ----------------------------------------------------------------
        // Deserialize
        // ----------------------------------------------------------------

        /// <summary>
        /// Deserializes a raw index key byte array (as produced by
        /// <see cref="Serialize(object[], ParadoxFile.FieldInfo[])"/>) back
        /// into field values. Used to reconstruct synthetic records from
        /// .PX/.Xnn/.Xgn leaf-entry key data during index lookups, since the
        /// on-disk key encoding for each field type is identical to the
        /// corresponding BinaryReaderWriterPdoxExtensions Read/Write pair
        /// (sign-flipped, big-endian for numeric types).
        /// </summary>
        public static object[] Deserialize(byte[] keyData, ParadoxFile.FieldInfo[] fields)
        {
            using (var ms = new MemoryStream(keyData))
            using (var r  = new BinaryReader(ms))
            {
                var values = new object[fields.Length];
                for (int i = 0; i < fields.Length; i++)
                    values[i] = ReadField(r, fields[i]);
                return values;
            }
        }

        private static object ReadField(BinaryReader r, ParadoxFile.FieldInfo field)
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

                default:
                    return r.ReadPdoxBytes(field.fSize);
            }
        }

        // ----------------------------------------------------------------
        // Compare
        // ----------------------------------------------------------------

        public static int Compare(byte[] a, byte[] b, ParadoxFile.FieldInfo[] fields)
        {
            int offset = 0;
            foreach (var field in fields)
            {
                int result = CompareField(a, b, offset, field);
                if (result != 0) return result;
                offset += field.fSize;
            }
            return 0;
        }

        private static int CompareField(byte[] a, byte[] b, int offset, ParadoxFile.FieldInfo field)
        {
            switch (field.fType)
            {
                case ParadoxFieldTypes.Alpha:
                {
                    string sa = Encoding.Default
                        .GetString(a, offset, field.fSize).TrimEnd('\0');
                    string sb = Encoding.Default
                        .GetString(b, offset, field.fSize).TrimEnd('\0');
                    return string.Compare(sa, sb, StringComparison.Ordinal);
                }

                case ParadoxFieldTypes.Short:
                {
                    ushort va = (ushort)((a[offset] << 8) | a[offset + 1]);
                    ushort vb = (ushort)((b[offset] << 8) | b[offset + 1]);
                    return va.CompareTo(vb);
                }

                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                case ParadoxFieldTypes.Date:
                case ParadoxFieldTypes.Time:
                {
                    uint va = ReadBigEndianUInt32(a, offset);
                    uint vb = ReadBigEndianUInt32(b, offset);
                    return va.CompareTo(vb);
                }

                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                case ParadoxFieldTypes.Timestamp:
                {
                    for (int i = 0; i < field.fSize; i++)
                    {
                        int cmp = a[offset + i].CompareTo(b[offset + i]);
                        if (cmp != 0) return cmp;
                    }
                    return 0;
                }

                case ParadoxFieldTypes.Logical:
                    return a[offset].CompareTo(b[offset]);

                case ParadoxFieldTypes.BCD:
                {
                    for (int i = 0; i < 17; i++)
                    {
                        int cmp = a[offset + i].CompareTo(b[offset + i]);
                        if (cmp != 0) return cmp;
                    }
                    return 0;
                }

                default:
                {
                    for (int i = 0; i < field.fSize; i++)
                    {
                        int cmp = a[offset + i].CompareTo(b[offset + i]);
                        if (cmp != 0) return cmp;
                    }
                    return 0;
                }
            }
        }

        // ----------------------------------------------------------------
        // Date / Timestamp helpers
        // ----------------------------------------------------------------

        public static int DateTimeToParadoxDate(DateTime dt)
            => (int)(dt.Date - new DateTime(1, 1, 1)).TotalDays + 1;

        public static double DateTimeToParadoxTimestamp(DateTime dt)
        {
            double days = (dt.Date - new DateTime(1, 1, 1)).TotalDays + 1;
            double ms   = dt.TimeOfDay.TotalMilliseconds;
            return days * 86400000.0 + ms;
        }

        // ----------------------------------------------------------------
        // Endian helpers
        // ----------------------------------------------------------------

        private static void WriteBigEndianUInt32(BinaryWriter w, uint value)
        {
            w.Write((byte)((value >> 24) & 0xFF));
            w.Write((byte)((value >> 16) & 0xFF));
            w.Write((byte)((value >>  8) & 0xFF));
            w.Write((byte)( value        & 0xFF));
        }

        private static uint ReadBigEndianUInt32(byte[] data, int offset)
            => (uint)((data[offset]     << 24)
                    | (data[offset + 1] << 16)
                    | (data[offset + 2] <<  8)
                    |  data[offset + 3]);
    }
}
