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
        // Which Paradox on-disk format level newly created tables use.
        // Historically this library always wrote 0x0B (Paradox 5/7 short
        // "compatible" format, 79-byte table-name layout), matching the
        // BDE's default idapi32.cfg PARADOX LEVEL=5 setting - confirmed
        // empirically across every real BDE-created fixture file in
        // ParadoxTest\data captured under that config (TESTTAB.DB/.PX and
        // its variants all have fileVersionID == 0x0B at offset 0x39).
        //
        // Byte-diffing tables created by a real, unmodified BDE (via
        // SQLRunner) under PARADOX LEVEL=5/BLOCK SIZE=2048 vs
        // PARADOX LEVEL=7/BLOCK SIZE=32768 (see
        // ParadoxTest.BdeConfigCompareTest) shows the *only* content
        // differences driven by that config change are: this fileVersionID
        // byte (0x0B -> 0x0C at offset 0x39, and the matching 0x0100|id
        // stamp in the V4 header), the maxTableSize byte (see
        // DefaultMaxTableSize below), and the resulting shift of every
        // byte after the table-name field once nameFieldLength grows from
        // 79 to 261 (which BuildHeader already handles via the
        // FileVersionId >= 0x0C check below) - no other structural changes
        // were observed for any of the 9 schema shapes tested (plain field,
        // PK-only, PK+alpha, PK+alpha+secondary index, AUTOINC variants,
        // memo, and BLOb fields), so 0x0C is safe to use as the new default
        // in order to support the larger 32768-byte block size (see
        // DefaultMaxTableSize) and its ~4GB max table size.
        private const byte FileVersionId = 0x0C;

        // A .PX/.XGn leaf entry's pointer overhead: blockNumber(2) +
        // recordCount(2) + reserved(2). Mirrors PrimaryIndexFile.POINTER_SIZE
        // / SecondaryIndexFile.POINTER_SIZE.
        private const int PxPointerSize = 6;

        // Which block size (in KB) newly created tables use, encoded as a
        // single byte at offset 0x05 (blockSize = maxTableSize * 1024).
        // Historically this library always wrote 2 (2048-byte blocks,
        // ~128MB max table size), matching the BDE's default idapi32.cfg
        // BLOCK SIZE=2048 setting - confirmed empirically across every real
        // BDE-created fixture file in ParadoxTest\data captured under that
        // config (TESTTAB.DB/.PX/.XG0-2 and its variants all have
        // maxTableSize == 2, independent of record size or field count).
        //
        // Per ParadoxTest.BdeConfigCompareTest (byte-diffing real
        // SQLRunner/BDE-created tables under BLOCK SIZE=2048 vs
        // BLOCK SIZE=32768), the only content difference this config
        // change drives is this byte (2 -> 32) plus the pointer/offset
        // fields that are naturally recomputed from it - so 32 (32768-byte
        // blocks, ~4GB max table size) is safe to use as the new default.
        private const byte DefaultMaxTableSize = 32; // 32768-byte blocks

        // .PX (primary index) and .YGn ("maintained field" companion) files
        // are the exception to DefaultMaxTableSize above: per
        // ParadoxTest.BdeConfigCompareTest, real BDE-created .PX/.YGn files
        // always use fixed 2048-byte blocks (maxTableSize == 2) regardless
        // of the table's configured BLOCK SIZE/DefaultMaxTableSize -
        // confirmed by comparing a real SQLRunner/BDE-created .PX/.YGn under
        // BLOCK SIZE=32768 (maxTableSize byte stayed 2) against their
        // sibling .DB/.XGn files (which did scale to 32). Only .DB and
        // .XGn/.Xnn/.Ynn files use DefaultMaxTableSize.
        private const byte FixedSmallMaxTableSize = 2; // 2048-byte blocks, always

        // BDE also always pads a freshly created (empty) file's header block
        // out to exactly 2048 bytes, rather than the minimal byte count
        // actually needed by the header fields + field defs + names, as this
        // library previously did. Confirmed empirically: every real
        // BDE-created fixture file (.DB/.PX/.XG0-2) has headerSize == 0x0800
        // (2048) regardless of its content length - and per
        // ParadoxTest.BdeConfigCompareTest this header-block size does NOT
        // scale with the data block size (maxTableSize/DefaultMaxTableSize
        // above): a real BDE table created with BLOCK SIZE=32768 still has
        // this exact same 2048-byte empty header block on disk (only the
        // *data* blocks that follow it are 32768 bytes each). So this must
        // stay a fixed 2048 constant, not maxTableSize * 0x400.
        private const int DefaultHeaderSize = 2 * 0x400;

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

            // A .PX file's RecordSize is NOT simply the sum of its key
            // fields' sizes - unlike a .DB file, where RecordSize is exactly
            // the row data length, a .PX leaf entry is keyData + a 6-byte
            // pointer (blockNumber(2) + recordCount(2) + reserved(2); see
            // PrimaryIndexFile.POINTER_SIZE/entrySize). Real BDE-created .PX
            // files' RecordSize header field reflects this entry size, not
            // the raw key size - confirmed empirically (e.g. an INTEGER
            // (4-byte) primary key produces RecordSize == 0x0A == 4 + 6, not
            // 0x04). Using the raw key size here previously produced a
            // RecordSize BDE apps like Paradox 7/Database Desktop reject as
            // corrupt ("Older version (see context)").
            int keyDataSize = keyFields.Sum(f => (int)f.Size);
            int pxRecordSize = keyDataSize + PxPointerSize;

            return BuildHeader(schema, ParadoxFileType.PxFile, keyFields, includeFieldNames: false, recordSizeOverride: pxRecordSize, maxTableSizeOverride: FixedSmallMaxTableSize);
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

            // Like .PX, real BDE-created .YGn "maintained field" companion
            // files always use fixed 2048-byte blocks (maxTableSize == 2)
            // regardless of the table's configured block size - confirmed
            // empirically (see ParadoxTest.BdeConfigCompareTest /
            // FixedSmallMaxTableSize remarks above); only the corresponding
            // .XGn index file itself scales with DefaultMaxTableSize.
            return BuildHeader(schema, ParadoxFileType.YgnFile, zeroedFields, includeFieldNames: false, indexFieldNumber: 0, maxTableSizeOverride: FixedSmallMaxTableSize);
        }

        /// <summary>
        /// Builds a complete empty (no blobs written yet) .MB blob file
        /// header block. Real BDE-created .MB files (even ones with zero
        /// memo/BLOb values ever written) always start with this exact
        /// 4096-byte header block - confirmed byte-for-byte identical
        /// across every real SQLRunner-created blank .MB fixture in
        /// ParadoxTest\data regardless of table name/field count
        /// (PKALPBLOB, PKALPMEMO, AUTOALPBLOB all produce an identical
        /// blank .MB). Only offsets 0x01, 0x03, 0x05-0x07, 0x09, 0x0C,
        /// 0x0E, 0x10-0x11, 0x14, 0x29-0x2A are non-zero; the remainder of
        /// the 4096-byte block (including the rest of this header block
        /// and its free-space bitmap) is zero for a brand-new blob file
        /// with no blobs written yet.
        /// </summary>
        public static byte[] BuildBlankMbHeader()
        {
            byte[] header = new byte[ParadoxHeaderOffsets.BlobHeaderBlockSize];
            header[0x01] = 0x01;
            header[0x03] = 0x01;
            header[0x05] = 0x82;
            header[0x06] = 0x73;
            header[0x07] = 0x02;
            header[0x09] = 0x29;
            header[0x0C] = 0x10;
            header[0x0E] = 0x10;
            header[0x10] = 0x10;
            header[0x11] = 0x40;
            header[0x14] = 0x08;
            header[0x29] = 0xA1;
            header[0x2A] = 0x02;
            return header;
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
            int indexFieldNumber = -1,
            int? recordSizeOverride = null,
            byte? maxTableSizeOverride = null)
        {
            byte maxTableSize = maxTableSizeOverride ?? DefaultMaxTableSize;
            using (var ms = new MemoryStream())
            using (var w = new BinaryWriter(ms, Encoding.ASCII))
            {
                int recordSize = recordSizeOverride ?? fields.Sum(f => (int)f.Size);
                int fieldCount = fields.Count;
                int primaryKeyCount = fields.Count(f => f.IsPrimaryKey);
                int nameFieldLength = FileVersionId >= 0x0C ? 261 : 79;

                // V4 header - only present for file types ParadoxFile.ReadHeader
                // actually reads one for (DB/.Xnn/.XGn - see its FileType
                // check), NOT .PX or .YGn. Previously this was written
                // unconditionally for every file type, which shifted every
                // .PX/.YGn file's field definitions, table name, and field
                // names 32 bytes out of position relative to what a real
                // BDE reader expects - producing structurally corrupt .PX
                // files that open fine via this library (which mirrors the
                // same - consistent, but wrong - layout) but get rejected by
                // real BDE apps like Paradox 7/Database Desktop as
                // "Older version (see context)".
                bool hasV4Header =
                    fileType == ParadoxFileType.DbFileIndexed ||
                    fileType == ParadoxFileType.DbFileNotIndexed ||
                    fileType == ParadoxFileType.XnnFileInc ||
                    fileType == ParadoxFileType.XnnFileNonInc ||
                    fileType == ParadoxFileType.XgnFileInc ||
                    fileType == ParadoxFileType.XgnFileNonInc;

                w.Write((ushort)recordSize);       // 0x00 RecordSize
                w.Write((ushort)0);                 // 0x02 headerSize (patched below)
                w.Write((byte)fileType);            // 0x04 FileType
                w.Write(maxTableSize);                // 0x05 maxTableSize
                w.Write(0);                          // 0x06 RecordCount
                w.Write((ushort)0);                  // 0x0A nextBlock
                w.Write((ushort)0);                  // 0x0C fileBlocks
                w.Write((ushort)0);                  // 0x0E firstBlock
                w.Write((ushort)0);                  // 0x10 lastBlock
                // 0x12-0x13: constant 0x0006 across every real BDE-created
                // fixture (DB and PX alike, regardless of schema) -
                // confirmed empirically.
                w.Write((ushort)6);                  // 0x12 unknown12x13
                w.Write((byte)0);                    // 0x14 modifiedFlags1
                // indexFieldNumber (0x15) is only meaningful on secondary
                // index (.Xnn/.XGn) files - real BDE-created .DB files
                // always have this byte == 0 regardless of primary-key
                // count (confirmed against every fixture in ParadoxTest\data,
                // all of which have a primary key yet 0x15 == 0x00).
                // Previously this defaulted to 1 whenever the file had a
                // primary key, incorrectly setting it on .DB/.PX files too.
                w.Write((byte)(indexFieldNumber >= 0 ? indexFieldNumber : 0)); // 0x15 indexFieldNumber
                w.Write(0);                          // 0x16 primaryIndexWorkspace
                w.Write(0);                          // 0x1A unknownPtr1A
                w.Write((ushort)0);                  // 0x1E pxRootBlockId
                w.Write((byte)0);                    // 0x20 pxLevelCount
                w.Write((short)fieldCount);          // 0x21 FieldCount
                // primaryKeyFields (0x23): only meaningful on .DB and
                // secondary-index (.Xnn/.XGn) files - real BDE-created .PX
                // (primary index) files always have this == 0, even though
                // every field in a .PX is itself a key field - confirmed via
                // binary-patch/diff against a real SQLRunner-created
                // PKONLY.PX (0x23 == 0x00 there vs our previous non-zero
                // value equal to the key field count) and consistent with
                // pxlibcpp's put_px_head(), which only sets primaryKeyFields
                // for pxfFileTyp*SecIndex* file types, never for
                // pxfFileTypPrimIndex.
                w.Write((short)(fileType == ParadoxFileType.PxFile ? 0 : primaryKeyCount)); // 0x23 primaryKeyFields
                // encryption1 (0x25) - real BDE .DB/.Xnn/.XGn files (no
                // password) always have 0x00 0xFF 0x00 0xFF here, not zero;
                // .PX/.YGn leave it zero when unencrypted (confirmed across
                // every ParadoxTest\data fixture). This library does not
                // support password-protected tables, so this is always the
                // unencrypted DB stamp / zero for PX.
                w.Write(hasV4Header ? new byte[] { 0x00, 0xFF, 0x00, 0xFF } : new byte[4]); // 0x25 encryption1
                w.Write((byte)0x4C);                 // 0x29 sortOrder
                w.Write((byte)0);                    // 0x2A modifiedFlags2
                // unknown2Bx2C[1] (0x2C): CRITICAL, load-bearing field on
                // .PX (primary index) files specifically - confirmed via
                // binary-patch experimentation against a real
                // BDE/SQLRunner-created PKONLY.PX fixture: leaving this at 0
                // (our previous value) makes BDE fail to read the table
                // (SQLRunner's "select count(*)" oracle fails / crashes).
                // pxlibcpp's put_px_head() claims unknown2Bx2C[1]=102
                // (0x66) unconditionally for pxfFileTypPrimIndex, but direct
                // binary-patch testing against a real SQLRunner/BDE-created
                // PKONLY.PX fixture disproves that: 102 still fails the
                // oracle, while the real fixture's actual byte value 171
                // (0xAB) is the value that passes. 0xAB is consistent across
                // every real SQLRunner-created .PX fixture in
                // ParadoxTest\data regardless of field count/type (verified
                // across AUTOALPB, AUTOALPI, AUTOALPM, PKALPBLO, PKALPHA,
                // PKALPHID, PKALPMEM, PKONLY), so it appears to be a fixed
                // stamp rather than content-derived.
                w.Write((byte)0);                                          // 0x2B unknown2Bx2C[0]
                w.Write((byte)(fileType == ParadoxFileType.PxFile ? 0xAB : 0)); // 0x2C unknown2Bx2C[1]
                // changeCount1/changeCount2 (0x2D/0x2E): CRITICAL, load-bearing
                // fields for .DB files specifically - confirmed via
                // binary-patch experimentation against a real BDE/SQLRunner
                // -created PKONLY.DB (indexed) fixture: zeroing these two
                // bytes causes the BDE engine to fail the SQLRunner count(*)
                // oracle. pxlibcpp's put_px_head() claims changeCount1=2,
                // changeCount2=1 unconditionally for
                // pxfFileTypIndexDB/pxfFileTypNonIndexDB, but direct
                // binary-patch testing against a real SQLRunner-created
                // fixture disproves that: 2/1 still fails the oracle, while
                // the real fixture's actual byte values 0x5D/0x5B are what
                // passes. These exact values (0x5D, 0x5B) are identical
                // across every real SQLRunner-created .DB fixture in
                // ParadoxTest\data regardless of field count/table
                // name/content (verified across AUTOALPB, AUTOALPI,
                // AUTOALPM, NOIDX, PKALPBLO, PKALPHA, PKALPHID, PKALPMEM,
                // PKONLY), so - like unknown2Bx2C[1] on .PX files - this
                // appears to be a fixed stamp rather than content-derived.
                bool isDbFile = fileType == ParadoxFileType.DbFileIndexed || fileType == ParadoxFileType.DbFileNotIndexed;
                w.Write((byte)(isDbFile ? 0x5D : 0));   // 0x2D changeCount1
                w.Write((byte)(isDbFile ? 0x5B : 0));   // 0x2E changeCount2
                w.Write((byte)0);                    // 0x2F unknown2F
                w.Write(0);                          // 0x30 tableNamePtrPtr
                w.Write(0);                          // 0x34 fldInfoPtr
                w.Write((byte)0);                    // 0x38 writeProtected
                w.Write(FileVersionId);               // 0x39 fileVersionID
                w.Write((ushort)0);                  // 0x3A maxBlocks
                w.Write((byte)0);                    // 0x3C unknown3C
                w.Write((byte)0);                    // 0x3D auxPasswords
                // 0x3E-0x3F: real BDE .DB/.Xnn/.XGn files always have
                // 0x0F1F here; .PX/.YGn files always have 0x0000 (same
                // hasV4Header-gated pattern as 0x56-0x57).
                w.Write(hasV4Header ? new byte[] { 0x1F, 0x0F } : new byte[2]); // 0x3E-0x3F unknown3Ex3F
                w.Write(0);                          // 0x40 cryptInfoStartPtr
                w.Write(0);                          // 0x44 cryptInfoEndPtr
                w.Write((byte)0);                    // 0x48 unknown48
                w.Write(0);                          // 0x49 autoIncVal
                w.Write(new byte[2]);                 // 0x4D-0x4E unknown4Dx4E
                w.Write((byte)0);                    // 0x4F indexUpdateRequired
                // 0x50: unknown/reserved - left zero (no stable pattern
                // observed across fixtures).
                w.Write((byte)0);                    // 0x50 unknown50
                // 0x51-0x52 (realHeaderSize, uint16): CRITICAL, load-bearing
                // field - confirmed via binary-patch experimentation against
                // a real BDE/SQLRunner-created NOIDX.DB: zeroing this field
                // on an otherwise-valid file makes the BDE engine crash with
                // an AccessViolationException on read (SQLRunner's
                // "select count(*)"), while restoring the correct value
                // fixes it. This mirrors pxlibcpp's put_px_head(), which
                // computes it as:
                //   dataheadoffset + numfields*(2+4+2) + 4 + tablenamelen
                //     + sumfieldlen + 9      (all types except .PX)
                //   dataheadoffset + numfields*2 + 4 + tablenamelen
                //     (.PX only)
                // where dataheadoffset is 0x78 when a V4 header is present,
                // else 0x58, and sumfieldlen is the sum of each field's
                // (name length + 1), or 1 per field when field names are not
                // written (matching pxlib's NULL-fname fallback).
                int dataHeadOffset = hasV4Header ? 0x78 : 0x58;
                int sumFieldLen = includeFieldNames
                    ? fields.Sum(f => Encoding.ASCII.GetByteCount(f.Name ?? string.Empty) + 1)
                    : fieldCount;
                int realHeaderSize = fileType == ParadoxFileType.PxFile
                    ? dataHeadOffset + fieldCount * 2 + 4 + nameFieldLength
                    : dataHeadOffset + fieldCount * (2 + 4 + 2) + 4 + nameFieldLength + sumFieldLen + 9;
                w.Write((ushort)realHeaderSize);     // 0x51-0x52 realHeaderSize
                w.Write((byte)0);                    // 0x53-0x54 unknown53x54 (only 1 byte remains here; see below)
                w.Write((byte)0);                    // pad to keep total 0x50-0x54 span at 5 bytes
                w.Write((byte)0);                    // 0x55 refIntegrity
                // 0x56-0x57: real BDE .DB/.Xnn/.XGn files always have 0x0020
                // here; .PX/.YGn files always have 0x0000 (confirmed across
                // every fixture in ParadoxTest\data).
                w.Write(hasV4Header ? new byte[] { 0x20, 0x00 } : new byte[2]); // 0x56-0x57 unknown56x57

                if (hasV4Header)
                {
                    // Real BDE always stamps fileVerID2/fileVerID3 with
                    // 0x010B (267) here, not 0 - confirmed across every real
                    // BDE-created fixture (testtab.DB, PKONLY.DB, etc, all
                    // regardless of field count/table name). This appears to
                    // be a fixed version stamp (high byte 0x01 + our
                    // FileVersionId 0x0B in the low byte), distinct from the
                    // single fileVersionID byte at 0x39. Leaving these as 0
                    // previously was one of the signals BDE apps use to
                    // reject freshly created files as "Older version (see
                    // context)".
                    short fileVerIdStamp = (short)(0x0100 | FileVersionId);
                    w.Write(fileVerIdStamp); // fileVerID2
                    w.Write(fileVerIdStamp); // fileVerID3
                    w.Write(0);          // encryption2
                    w.Write(0);          // fileUpdateTime
                    w.Write((ushort)0);  // hiFieldID
                    w.Write((ushort)0);  // hiFieldIDinfo
                    w.Write((short)0);   // sometimesNumFields
                    w.Write((ushort)1252);  // dosCodePage
                    // unknown6Cx6F[0..1] - real BDE/pxlib DB writers always
                    // stamp these two bytes as 0x01, 0x01 for .DB files (both
                    // indexed and non-indexed) - confirmed both by pxlibcpp's
                    // put_px_head() (unknown6Cx6F[0]=1, [1]=1 for
                    // pxfFileTypIndexDB/pxfFileTypNonIndexDB) and empirically
                    // against a real SQLRunner/BDE-created NOIDX.DB fixture
                    // (bytes 0x6C-0x6D == 01 01, vs our previous 00 00).
                    // Leaving these zero is a deterministic mismatch from
                    // every real DB-type file, unlike the surrounding pointer
                    // fields (0x60-0x66, 0x7A-0x89, etc) which are per-process
                    // memory addresses/timestamps and not stable across runs.
                    w.Write((byte)0x01); // unknown6Cx6F[0]
                    w.Write((byte)0x01); // unknown6Cx6F[1]
                    w.Write(new byte[2]); // unknown6Cx6F[2..3]
                    w.Write((short)0);   // changeCount4
                    w.Write(new byte[6]); // unknown72x77
                }

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

                    // fieldNumbers array (one 1-based uint16 per field) +
                    // sortOrderID (8-byte ASCII language-driver identifier).
                    // Real BDE-created .DB files always write these two
                    // sections immediately after the field names - confirmed
                    // empirically against a real SQLRunner/BDE-created
                    // PKONLY.DB fixture (bytes 0xD4-0xDD == 01 00 44 42 57 49
                    // 4E 55 53 30, i.e. fieldNumber=1 followed by ASCII
                    // "DBWINUS0"), and mirrored by pxlibcpp's put_px_head()
                    // (which writes the analogous fieldNumbers + an 8-byte
                    // sortOrderID string for non-index files). Only present
                    // for .DB files (includeFieldNames == true); .PX/.XGn/
                    // .YGn files never write this section (isindex == true
                    // in pxlibcpp).
                    for (int i = 0; i < fieldCount; i++)
                        w.Write((ushort)(i + 1));

                    var sortOrderIdBytes = Encoding.ASCII.GetBytes("DBWINUS0");
                    w.Write(sortOrderIdBytes);
                    w.Write((byte)0); // trailing null (accounted for by the realHeaderSize "+9" above)
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
