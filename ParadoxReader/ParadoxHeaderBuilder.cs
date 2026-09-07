using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    /// <summary>
    /// Builds brand-new Paradox .DB/.PX header + field-definition bytes from
    /// a <see cref="TableSchemaDefinition"/>, mirroring the exact byte layout
    /// <see cref="ParadoxFile.ReadHeader"/> expects to read back. This is the
    /// one place responsible for synthesizing a table's structure "from
    /// nothing" (used by both <see cref="TableCreator"/> for File > New >
    /// Table, and <see cref="TableRebuilder.RebuildWithSchema"/> for Table >
    /// Modify Structure, which regenerates a table under a new schema).
    /// </summary>
    /// <remarks>
    /// Reserved/unknown header fields (pointers, workspace fields, etc.) are
    /// written as zero - they are never dereferenced by this library, only
    /// skipped over positionally by <see cref="ParadoxFile.ReadHeader"/>, so
    /// zero is a safe, valid value for a freshly created file.
    /// </remarks>
    internal static class ParadoxHeaderBuilder
    {
        // BDE always creates fresh tables with fileVersionID == 0x0B (Paradox
        // 7 format, 79-byte short table-name layout), never 0x0C - confirmed
        // empirically across every real BDE-created fixture file in
        // ParadoxTest\data (TESTTAB.DB/.PX and its variants all have
        // fileVersionID == 0x0B at offset 0x39). Using 0x0C caused readers
        // like BB's Database Desktop to reject freshly created files with
        // "Older version (see context)", since files claiming the 0x0C
        // (long table-name) layout must be laid out differently than what
        // this builder was producing.
        private const byte FileVersionId = 0x0B;

        // BDE always creates fresh tables with maxTableSize = 2 (2048-byte
        // blocks), never 1 (1024-byte blocks), regardless of schema -
        // confirmed empirically across every real BDE-created fixture file
        // in ParadoxTest\data (TESTTAB.DB/.PX/.XG0-2 and its variants): all
        // of them have maxTableSize == 2 at offset 0x05, independent of
        // record size or field count.
        private const byte DefaultMaxTableSize = 2; // 2048-byte blocks

        // BDE also always pads a freshly created file's header out to
        // exactly one full block (headerSize == maxTableSize * 0x400 == 2048
        // for the default maxTableSize above), rather than using the
        // minimal byte count actually needed by the header fields + field
        // defs + names, as this library previously did. Confirmed
        // empirically: every real BDE-created fixture file (.DB/.PX/.XG0-2)
        // has headerSize == 0x0800 (2048) regardless of its content length.
        private const int DefaultHeaderSize = DefaultMaxTableSize * 0x400;

        private const int V4HeaderSize = 32;

        /// <summary>
        /// Builds a complete empty (zero-record) .DB file header + field
        /// definitions + field names for <paramref name="schema"/>.
        /// </summary>
        public static byte[] BuildDbHeader(TableSchemaDefinition schema)
        {
            var fileType = schema.PrimaryKeyFieldCount > 0
                ? ParadoxFileType.DbFileIndexed
                : ParadoxFileType.DbFileNotIndexed;

            return BuildHeader(schema, fileType, schema.Fields, includeFieldNames: true);
        }

        /// <summary>
        /// Builds a complete empty (zero-record, zero-level) .PX primary
        /// index file header + field definitions for the primary-key fields
        /// of <paramref name="schema"/>. Only valid when
        /// <see cref="TableSchemaDefinition.PrimaryKeyFieldCount"/> is > 0.
        /// </summary>
        public static byte[] BuildPxHeader(TableSchemaDefinition schema)
        {
            var keyFields = schema.Fields.Where(f => f.IsPrimaryKey).ToList();
            return BuildHeader(schema, ParadoxFileType.PxFile, keyFields, includeFieldNames: false);
        }

        /// <summary>
        /// Builds a complete empty (zero-record, one empty root block) .XGn
        /// secondary index file header + field definitions for
        /// <paramref name="index"/> (mirroring the on-disk field composition
        /// Paradox uses: indexed field(s) followed by the table's
        /// primary-key field(s), followed by one trailing 2-byte Short field
        /// that Paradox always appends and which is neither an indexed nor
        /// PK field).
        /// </summary>
        /// <remarks>
        /// Real BDE-created "quick" indexes (created interactively without a
        /// name, e.g. via Table > Restructure's index column checkboxes) use
        /// .Xnn/.Ynn naming where nn is the hex 1-based indexed-field
        /// position - confirmed against a real installed application's data
        /// (C:\medilink32bnt\data\!transac.X06 etc). However, SQLRunner's
        /// CREATE INDEX (equivalent to what TableCreator/TableRebuilder
        /// synthesize from a TableIndexDefinition - always a *named* index)
        /// produces .XGn/.YGn regardless of field count - confirmed by
        /// comparing SQLRunner's CREATE INDEX NAMEIDX ON t (NAME) output
        /// against our creation path in the headercomparetest harness, which
        /// showed SQLRunner emitting .XG0/.YG0 (fileType=0x08) for a
        /// single-field named index, not .X02/.Y02. Since every index this
        /// library creates corresponds to a named index, .XGn/.YGn (with a
        /// sequential ordinal) is always correct here.
        /// </remarks>
        public static byte[] BuildSecondaryIndexHeader(TableSchemaDefinition schema, TableIndexDefinition index)
        {
            var fields = BuildSecondaryIndexFieldList(schema, index);

            // Real BDE always appends one trailing 2-byte Short field to
            // every .Xnn/.XGn index (see SecondaryIndexDiscovery remarks) -
            // confirmed empirically (e.g. !transac.X06 has 3 fields: 1
            // indexed + 1 PK + this trailing Short).
            fields.Add(new TableFieldDefinition(string.Empty, ParadoxFieldTypes.Short, 2, false));

            int indexFieldNumber = index.FieldIndices[0] + 1; // 1-based

            // No root block is pre-allocated: SecondaryIndexFile.OnBlockChanged
            // already detects an empty index (stream.Length <= headerSize ||
            // RecordCount <= 0) and allocates the first leaf block itself on
            // the very first insert, exactly as it does for a freshly
            // rebuilt/cloned index skeleton (see TableRebuilder).
            return BuildHeader(schema, ParadoxFileType.XgnFileInc, fields, includeFieldNames: false, indexFieldNumber: indexFieldNumber);
        }

        /// <summary>
        /// Builds the .YGn "maintained field" companion header for
        /// <paramref name="index"/> - one field short of the corresponding
        /// .XGn index (its trailing Short field is dropped), with all field
        /// definitions zeroed. Confirmed empirically against real
        /// BDE-created files (e.g. !transac.Y06/.YG0): field defs are all
        /// type=0/size=0 placeholders, and indexFieldNumber (0x15) is always
        /// 0 on these companion files.
        /// </summary>
        public static byte[] BuildMaintainedFieldHeader(TableSchemaDefinition schema, TableIndexDefinition index)
        {
            var fields = BuildSecondaryIndexFieldList(schema, index);
            var zeroedFields = fields.Select(f => new TableFieldDefinition(string.Empty, (ParadoxFieldTypes)0, 0, false)).ToList();

            return BuildHeader(schema, ParadoxFileType.YgnFile, zeroedFields, includeFieldNames: false, indexFieldNumber: 0);
        }

        private static List<TableFieldDefinition> BuildSecondaryIndexFieldList(TableSchemaDefinition schema, TableIndexDefinition index)
        {
            var fields = new List<TableFieldDefinition>();
            foreach (var i in index.FieldIndices)
                fields.Add(schema.Fields[i]);

            foreach (var f in schema.Fields.Where(f => f.IsPrimaryKey))
            {
                if (!fields.Contains(f))
                    fields.Add(f);
            }

            return fields;
        }

        private static byte[] BuildHeader(
            TableSchemaDefinition schema,
            ParadoxFileType fileType,
            List<TableFieldDefinition> fields,
            bool includeFieldNames,
            int indexFieldNumber = -1)
        {
            using (var ms = new MemoryStream())
            using (var w = new BinaryWriter(ms, Encoding.ASCII))
            {
                int recordSize = fields.Sum(f => (int)f.Size);
                int fieldCount = fields.Count;
                int primaryKeyCount = fields.Count(f => f.IsPrimaryKey);
                int nameFieldLength = FileVersionId >= 0x0C ? 261 : 79;

                w.Write((ushort)recordSize);       // 0x00 RecordSize
                w.Write((ushort)0);                 // 0x02 headerSize (patched below)
                w.Write((byte)fileType);            // 0x04 FileType
                w.Write(DefaultMaxTableSize);        // 0x05 maxTableSize
                w.Write(0);                          // 0x06 RecordCount
                w.Write((ushort)0);                  // 0x0A nextBlock
                w.Write((ushort)0);                  // 0x0C fileBlocks
                w.Write((ushort)0);                  // 0x0E firstBlock
                w.Write((ushort)0);                  // 0x10 lastBlock
                w.Write((ushort)0);                  // 0x12 unknown12x13
                w.Write((byte)0);                    // 0x14 modifiedFlags1
                w.Write((byte)(indexFieldNumber >= 0 ? indexFieldNumber : (primaryKeyCount > 0 ? 1 : 0))); // 0x15 indexFieldNumber
                w.Write(0);                          // 0x16 primaryIndexWorkspace
                w.Write(0);                          // 0x1A unknownPtr1A
                w.Write((ushort)0);                  // 0x1E pxRootBlockId
                w.Write((byte)0);                    // 0x20 pxLevelCount
                w.Write((short)fieldCount);          // 0x21 FieldCount
                w.Write((short)primaryKeyCount);     // 0x23 primaryKeyFields
                w.Write(0);                          // 0x25 encryption1
                w.Write((byte)0);                    // 0x29 sortOrder
                w.Write((byte)0);                    // 0x2A modifiedFlags2
                w.Write(new byte[2]);                 // 0x2B-0x2C unknown2Bx2C
                w.Write((byte)0);                    // 0x2D changeCount1
                w.Write((byte)0);                    // 0x2E changeCount2
                w.Write((byte)0);                    // 0x2F unknown2F
                w.Write(0);                          // 0x30 tableNamePtrPtr
                w.Write(0);                          // 0x34 fldInfoPtr
                w.Write((byte)0);                    // 0x38 writeProtected
                w.Write(FileVersionId);               // 0x39 fileVersionID
                w.Write((ushort)0);                  // 0x3A maxBlocks
                w.Write((byte)0);                    // 0x3C unknown3C
                w.Write((byte)0);                    // 0x3D auxPasswords
                w.Write(new byte[2]);                 // 0x3E-0x3F unknown3Ex3F
                w.Write(0);                          // 0x40 cryptInfoStartPtr
                w.Write(0);                          // 0x44 cryptInfoEndPtr
                w.Write((byte)0);                    // 0x48 unknown48
                w.Write(0);                          // 0x49 autoIncVal
                w.Write(new byte[2]);                 // 0x4D-0x4E unknown4Dx4E
                w.Write((byte)0);                    // 0x4F indexUpdateRequired
                w.Write(new byte[5]);                 // 0x50-0x54 unknown50x54
                w.Write((byte)0);                    // 0x55 refIntegrity
                w.Write(new byte[2]);                 // 0x56-0x57 unknown56x57

                // V4 header (only DB/index file types, fileVersionID >= 5 - always true here)
                w.Write((short)0);   // fileVerID2
                w.Write((short)0);   // fileVerID3
                w.Write(0);          // encryption2
                w.Write(0);          // fileUpdateTime
                w.Write((ushort)0);  // hiFieldID
                w.Write((ushort)0);  // hiFieldIDinfo
                w.Write((short)0);   // sometimesNumFields
                w.Write((ushort)0);  // dosCodePage
                w.Write(new byte[4]); // unknown6Cx6F
                w.Write((short)0);   // changeCount4
                w.Write(new byte[6]); // unknown72x77

                // Field definitions (fType byte + fSize byte per field)
                foreach (var f in fields)
                {
                    w.Write((byte)f.Type);
                    w.Write(f.Size);
                }

                w.Write(0); // tableNamePtr

                if (includeFieldNames)
                {
                    // fieldNamePtrArray (one int32 per field - unused positions, zero is fine)
                    for (int i = 0; i < fieldCount; i++)
                        w.Write(0);
                }

                // Table name (null-terminated ASCII, padded to nameFieldLength)
                var tableNameBytes = new byte[nameFieldLength];
                var nameBytes = Encoding.ASCII.GetBytes(schema.TableName ?? string.Empty);
                Array.Copy(nameBytes, tableNameBytes, Math.Min(nameBytes.Length, nameFieldLength - 1));
                w.Write(tableNameBytes);

                if (includeFieldNames)
                {
                    foreach (var f in fields)
                    {
                        var fieldNameBytes = Encoding.ASCII.GetBytes(f.Name ?? string.Empty);
                        w.Write(fieldNameBytes);
                        w.Write((byte)0);
                    }
                }

                w.Flush();
                byte[] content = ms.ToArray();

                // Pad the header out to the full block size BDE always uses
                // (see DefaultHeaderSize remarks above), rather than the
                // minimal length actually written above. Real BDE-created
                // files never have a headerSize shorter than a full block,
                // even when the actual header content is much smaller.
                byte[] header = new byte[Math.Max(content.Length, DefaultHeaderSize)];
                Array.Copy(content, header, content.Length);

                // Patch headerSize now that the true (padded) length is known.
                byte[] headerSizeBytes = BitConverter.GetBytes((ushort)header.Length);
                header[ParadoxHeaderOffsets.HeaderSize] = headerSizeBytes[0];
                header[ParadoxHeaderOffsets.HeaderSize + 1] = headerSizeBytes[1];

                return header;
            }
        }
    }
}
