using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Read-only reader for dBase (.DBF) tables: dBase III/IV/5, dBase 7 and
    /// FoxPro/Visual FoxPro variants. Memo fields are read from the companion
    /// .DBT (dBase) or .FPT (FoxPro) file when present. Deleted rows are skipped.
    /// </summary>
    public sealed class DbfReader
    {
        public sealed class DbfField
        {
            public string Name { get; internal set; }
            /// <summary>dBase type letter: C, N, F, D, L, M, I, Y, T, B, O, @, +, G, P, etc.</summary>
            public char Type { get; internal set; }
            public int Length { get; internal set; }
            public int DecimalCount { get; internal set; }
            internal int Offset;

            /// <summary>CLR type this field's values are returned as.</summary>
            public Type ClrType { get; internal set; }
        }

        private readonly byte[] data;
        private readonly int headerLength;
        private readonly int recordLength;
        private readonly bool isFoxPro;
        private readonly bool isVisualFoxPro;
        private readonly bool dBase7;
        private readonly string memoPath;
        private byte[] memo;

        public string FilePath { get; }
        public byte Version { get; }
        public int RecordCount { get; }
        public IReadOnlyList<DbfField> Fields { get; }
        public Encoding Encoding { get; }

        public DbfReader(string filePath, Encoding encoding = null)
        {
            FilePath = filePath;
            using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                data = new byte[fs.Length];
                int read = 0, n;
                while (read < data.Length && (n = fs.Read(data, read, data.Length - read)) > 0) read += n;
            }
            if (data.Length < 32) throw new InvalidDataException("File is too small to be a DBF table.");

            Version        = data[0];
            RecordCount    = BitConverter.ToInt32(data, 4);
            headerLength   = BitConverter.ToUInt16(data, 8);
            recordLength   = BitConverter.ToUInt16(data, 10);
            isVisualFoxPro = Version == 0x30 || Version == 0x31 || Version == 0x32;
            isFoxPro       = isVisualFoxPro || Version == 0xF5 || Version == 0xFB;
            dBase7         = (Version & 0x07) == 4;
            Encoding       = encoding ?? EncodingFromLanguageDriver(data[29]);

            var fields = new List<DbfField>();
            int descriptorSize = dBase7 ? 48 : 32;
            int pos = dBase7 ? 68 : 32;
            int offset = 1; // byte 0 of each record is the deletion flag
            while (pos + descriptorSize <= headerLength && pos < data.Length && data[pos] != 0x0D)
            {
                int nameLen = dBase7 ? 32 : 11;
                string name = Encoding.ASCII.GetString(data, pos, nameLen);
                int nul = name.IndexOf('\0');
                if (nul >= 0) name = name.Substring(0, nul);
                int typePos = pos + (dBase7 ? 32 : 11);
                var f = new DbfField
                {
                    Name = name.Trim(),
                    Type = (char)data[typePos],
                    Length = data[typePos + (dBase7 ? 1 : 5)],
                    DecimalCount = data[typePos + (dBase7 ? 2 : 6)],
                };
                if (f.Type == 'C' && !dBase7 && isFoxPro) f.Length |= f.DecimalCount << 8; // FoxPro long char fields
                f.Offset = offset;
                f.ClrType = ClrTypeFor(f);
                offset += f.Length;
                fields.Add(f);
                pos += descriptorSize;
            }
            Fields = fields;

            string dir = Path.GetDirectoryName(filePath) ?? string.Empty;
            string baseName = Path.GetFileNameWithoutExtension(filePath);
            foreach (var ext in isFoxPro ? new[] { ".fpt", ".dbt" } : new[] { ".dbt", ".fpt" })
            {
                var candidate = Path.Combine(dir, baseName + ext);
                if (File.Exists(candidate)) { memoPath = candidate; break; }
            }
        }

        private Type ClrTypeFor(DbfField f)
        {
            switch (f.Type)
            {
                case 'N':
                case 'F':
                    return f.DecimalCount == 0 && f.Length <= 9 ? typeof(int)
                         : f.DecimalCount == 0 && f.Length <= 18 ? typeof(long)
                         : typeof(decimal);
                case 'I':
                case '+': return typeof(int);
                case 'Y': return typeof(decimal);
                case 'B': return isVisualFoxPro ? typeof(double) : typeof(string);
                case 'O': return typeof(double);
                case 'D': return typeof(DateTime);
                case 'T':
                case '@': return typeof(DateTime);
                case 'L': return typeof(bool);
                default:  return typeof(string);
            }
        }

        /// <summary>Enumerates non-deleted records; each array has one value per field (DBNull.Value for blanks).</summary>
        public IEnumerable<object[]> Enumerate()
        {
            for (int r = 0; r < RecordCount; r++)
            {
                long start = headerLength + (long)r * recordLength;
                if (start + recordLength > data.Length) yield break;
                if (data[start] == (byte)'*') continue;
                var values = new object[Fields.Count];
                for (int i = 0; i < Fields.Count; i++)
                {
                    try { values[i] = ReadValue(Fields[i], (int)start + Fields[i].Offset) ?? DBNull.Value; }
                    catch { values[i] = DBNull.Value; }
                }
                yield return values;
            }
        }

        /// <summary>Loads all non-deleted records into a <see cref="DataTable"/>.</summary>
        public DataTable ToDataTable()
        {
            var table = new DataTable(Path.GetFileNameWithoutExtension(FilePath));
            foreach (var f in Fields)
            {
                string name = f.Name;
                for (int k = 2; table.Columns.Contains(name); k++) name = f.Name + "_" + k;
                table.Columns.Add(name, f.ClrType);
            }
            table.BeginLoadData();
            foreach (var row in Enumerate()) table.Rows.Add(row);
            table.EndLoadData();
            return table;
        }

        private object ReadValue(DbfField f, int p)
        {
            switch (f.Type)
            {
                case 'C':
                {
                    string s = Encoding.GetString(data, p, f.Length).TrimEnd(' ', '\0');
                    return s;
                }
                case 'N':
                case 'F':
                {
                    string s = Encoding.ASCII.GetString(data, p, f.Length).Trim().Trim('\0');
                    if (s.Length == 0 || s.StartsWith("*")) return null;
                    decimal d;
                    if (!decimal.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out d)) return null;
                    if (f.ClrType == typeof(int)) return (int)d;
                    if (f.ClrType == typeof(long)) return (long)d;
                    return d;
                }
                case 'D':
                {
                    string s = Encoding.ASCII.GetString(data, p, 8).Trim();
                    DateTime dt;
                    return s.Length == 8 && DateTime.TryParseExact(s, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out dt)
                        ? (object)dt : null;
                }
                case 'L':
                {
                    char c = char.ToUpperInvariant((char)data[p]);
                    if (c == 'T' || c == 'Y') return true;
                    if (c == 'F' || c == 'N') return false;
                    return null;
                }
                case 'I':
                    // dBase 7 longs are big-endian with the sign bit flipped; FoxPro integers are little-endian.
                    if (dBase7) return (int)(ReadBigEndianUInt32(p) ^ 0x80000000);
                    return BitConverter.ToInt32(data, p);
                case '+':
                    return (int)(ReadBigEndianUInt32(p) ^ 0x80000000);
                case 'Y':
                    return BitConverter.ToInt64(data, p) / 10000m;
                case 'O':
                {
                    // dBase 7 double: big-endian, sign bit flipped for positive, all bits inverted for negative.
                    var b = new byte[8];
                    Array.Copy(data, p, b, 0, 8);
                    if ((b[0] & 0x80) != 0) b[0] &= 0x7F; else for (int i = 0; i < 8; i++) b[i] = (byte)~b[i];
                    Array.Reverse(b);
                    return BitConverter.ToDouble(b, 0);
                }
                case 'T':
                {
                    int julian = BitConverter.ToInt32(data, p);
                    int ms = BitConverter.ToInt32(data, p + 4);
                    if (julian == 0) return null;
                    return new DateTime(1, 1, 1).AddDays(julian - 1721426).AddMilliseconds(ms);
                }
                case '@':
                {
                    var b = new byte[8];
                    Array.Copy(data, p, b, 0, 8);
                    if ((b[0] & 0x80) != 0) b[0] &= 0x7F; else for (int i = 0; i < 8; i++) b[i] = (byte)~b[i];
                    Array.Reverse(b);
                    double msSinceEpoch = BitConverter.ToDouble(b, 0);
                    if (msSinceEpoch == 0) return null;
                    return new DateTime(1, 1, 1).AddDays(-1).AddMilliseconds(msSinceEpoch);
                }
                case 'B':
                    if (isVisualFoxPro) return BitConverter.ToDouble(data, p);
                    return ReadMemo(f, p);
                case 'M':
                case 'G':
                case 'P':
                    return ReadMemo(f, p);
                default:
                    return Encoding.GetString(data, p, f.Length).TrimEnd(' ', '\0');
            }
        }

        private uint ReadBigEndianUInt32(int p) =>
            (uint)(data[p] << 24 | data[p + 1] << 16 | data[p + 2] << 8 | data[p + 3]);

        private string ReadMemo(DbfField f, int p)
        {
            long block;
            if (f.Length == 4) block = BitConverter.ToUInt32(data, p);
            else
            {
                string s = Encoding.ASCII.GetString(data, p, f.Length).Trim().Trim('\0');
                if (s.Length == 0 || !long.TryParse(s, out block)) return null;
            }
            if (block <= 0 || memoPath == null) return null;
            if (memo == null)
            {
                using (var fs = new FileStream(memoPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    memo = new byte[fs.Length];
                    int read = 0, n;
                    while (read < memo.Length && (n = fs.Read(memo, read, memo.Length - read)) > 0) read += n;
                }
            }

            if (memoPath.EndsWith(".fpt", StringComparison.OrdinalIgnoreCase))
            {
                int blockSize = memo[6] << 8 | memo[7];
                if (blockSize == 0) blockSize = 64;
                long start = block * blockSize;
                if (start + 8 > memo.Length) return null;
                int len = memo[start + 4] << 24 | memo[start + 5] << 16 | memo[start + 6] << 8 | memo[start + 7];
                len = (int)Math.Min(len, memo.Length - start - 8);
                return len <= 0 ? string.Empty : Encoding.GetString(memo, (int)start + 8, len);
            }

            // dBase IV+ DBT: block size at offset 20; each entry starts FF FF 08 00 + int32 length (incl. 8-byte header).
            int dbtBlockSize = memo.Length > 22 ? BitConverter.ToUInt16(memo, 20) : 0;
            if (Version == 0x03 || Version == 0x83 || dbtBlockSize == 0) dbtBlockSize = 512;
            long pos = block * dbtBlockSize;
            if (pos >= memo.Length) return null;
            if (pos + 8 <= memo.Length && memo[pos] == 0xFF && memo[pos + 1] == 0xFF && memo[pos + 2] == 0x08 && memo[pos + 3] == 0x00)
            {
                int len = BitConverter.ToInt32(memo, (int)pos + 4) - 8;
                len = (int)Math.Min(len, memo.Length - pos - 8);
                return len <= 0 ? string.Empty : Encoding.GetString(memo, (int)pos + 8, len);
            }
            // dBase III: text terminated by 0x1A (0x1A 0x1A).
            long end = pos;
            while (end < memo.Length && memo[end] != 0x1A) end++;
            return Encoding.GetString(memo, (int)pos, (int)(end - pos));
        }

        private static Encoding EncodingFromLanguageDriver(byte ldid)
        {
            int codePage;
            switch (ldid)
            {
                case 0x01: codePage = 437; break;
                case 0x02: codePage = 850; break;
                case 0x03: codePage = 1252; break;
                case 0x57: codePage = 1252; break;
                case 0x64: codePage = 852; break;
                case 0x65: codePage = 866; break;
                case 0x66: codePage = 865; break;
                case 0x7D: codePage = 1255; break;
                case 0x7E: codePage = 1256; break;
                case 0xC8: codePage = 1250; break;
                case 0xC9: codePage = 1251; break;
                case 0xCA: codePage = 1254; break;
                case 0xCB: codePage = 1253; break;
                default: return Encoding.Default;
            }
            try { return Encoding.GetEncoding(codePage); }
            catch { return Encoding.Default; }
        }
    }
}
