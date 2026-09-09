using System.IO;

namespace ParadoxReader
{
    public partial class ParadoxFile
    {
        /// <summary>
        /// The 32-byte "V4" header extension, physically located right after
        /// the base header's unknown56x57 field (i.e. starting at absolute
        /// offset 0x58 in the file, see <see cref="ParadoxFile.ReadHeader"/>).
        /// </summary>
        /// <remarks>
        /// <para>
        /// PRESENCE: only physically written/read for .DB/.Xnn/.XGn file
        /// types with fileVersionID &gt;= 5 (see the FileType/fileVersionID
        /// check around <see cref="ParadoxFile.ReadHeader"/>'s
        /// <c>this.V4Header = new V4Hdr(r)</c> call, and the matching
        /// <c>hasV4Header</c> gate in <see cref="ParadoxHeaderBuilder"/>).
        /// .PX and .YGn files never have this block - confirmed empirically
        /// across every real BDE-created fixture in ParadoxTest\data, and
        /// (independently) across every real BDE-created fixture generated
        /// under both idapi32.cfg PARADOX LEVEL=5/BLOCK SIZE=2048 and
        /// LEVEL=7/BLOCK SIZE=32768 (see ParadoxTest.BdeConfigCompareTest).
        /// </para>
        /// <para>
        /// KNOWN VALUES a writer needs to set (all confirmed against real
        /// BDE/SQLRunner-created fixtures, offsets given as absolute file
        /// offsets):
        /// </para>
        /// <list type="bullet">
        /// <item><description>
        /// fileVerID2/fileVerID3 (0x58-0x5B): NOT zero. Real BDE always
        /// stamps both as <c>0x0100 | fileVersionID</c> (e.g. 0x010B for
        /// fileVersionID 0x0B "Paradox 5/7 short name" format, 0x010C for
        /// fileVersionID 0x0C "Paradox 7+ long name" format) - i.e. high
        /// byte 0x01 + the same single fileVersionID byte already written
        /// at absolute offset 0x39 in the base header. Leaving these zero
        /// is one of the signals real BDE apps (e.g. Database Desktop) use
        /// to reject a freshly created file as "Older version (see
        /// context)".
        /// </description></item>
        /// <item><description>
        /// encryption2 (0x5C-0x5F), fileUpdateTime (0x60-0x63): 0 for a
        /// freshly created table with no encryption/prior saves.
        /// </description></item>
        /// <item><description>
        /// hiFieldID/hiFieldIDinfo (0x64-0x67), sometimesNumFields
        /// (0x68-0x69): 0 for a freshly created table (this library writes
        /// zero here; real BDE has been observed writing non-zero values in
        /// this region on some fixtures, but a reader/writer round-trip
        /// with zero here has not been observed to cause any rejection -
        /// unlike fileVerID2/fileVerID3 above).
        /// </description></item>
        /// <item><description>
        /// dosCodePage (0x6A-0x6B): 1252 (Windows-1252/ANSI Latin 1) for
        /// every real BDE-created fixture examined.
        /// </description></item>
        /// <item><description>
        /// unknown6Cx6F[0..1] (0x6C-0x6D): NOT zero for .DB files - real
        /// BDE/pxlib always stamp 0x01, 0x01 here for both indexed and
        /// non-indexed .DB files (confirmed both by pxlibcpp's
        /// put_px_head(), which sets these for pxfFileTypIndexDB/
        /// pxfFileTypNonIndexDB, and empirically against real
        /// SQLRunner/BDE-created .DB fixtures). unknown6Cx6F[2..3]
        /// (0x6E-0x6F) can be left zero.
        /// </description></item>
        /// <item><description>
        /// changeCount4 (0x70-0x71, exposed as <see cref="changeCount4"/>):
        /// the "table version"/write counter for this file - 0 for a
        /// freshly created empty table, incremented on structural writes.
        /// See <see cref="ParadoxHeaderOffsets.ChangeCount4"/> and the
        /// remarks on <c>PrimaryIndexFile</c>/<c>SecondaryIndexFile</c>
        /// about how it no longer needs to mirror the parent .DB file's
        /// value.
        /// </description></item>
        /// <item><description>
        /// unknown72x77 (0x72-0x77): 0 for a freshly created table - no
        /// stable non-zero pattern observed across fixtures.
        /// </description></item>
        /// </list>
        /// <para>
        /// See <see cref="ParadoxHeaderBuilder"/>'s <c>BuildHeader</c> for
        /// the authoritative from-scratch writer implementing all of the
        /// above, and <c>ParadoxTest.BdeConfigCompareTest</c>/
        /// <c>HeaderCompareTest</c> for the byte-diffing harnesses used to
        /// confirm these values against a real, unmodified BDE.
        /// </para>
        /// </remarks>
        public class V4Hdr
        {
            /// <summary>fileVerID2 (int16) @ 0x58. See class remarks - NOT zero, must be 0x0100 | fileVersionID.</summary>
            short fileVerID2;

            /// <summary>fileVerID3 (int16) @ 0x5A. Mirrors fileVerID2 - see class remarks.</summary>
            short fileVerID3;

            /// <summary>encryption2 (int32) @ 0x5C. 0 for an unencrypted freshly created table.</summary>
            internal int encryption2;
            public int Encryption2 => encryption2;

            /// <summary>fileUpdateTime (int32) @ 0x60. 4.0 only; 0 for a freshly created table.</summary>
            int fileUpdateTime;  // 4.0 only

            /// <summary>hiFieldID (uint16) @ 0x64. 0 for a freshly created table.</summary>
            ushort hiFieldID;

            /// <summary>hiFieldIDinfo (uint16) @ 0x66. 0 for a freshly created table.</summary>
            ushort hiFieldIDinfo;

            /// <summary>sometimesNumFields (int16) @ 0x68. 0 for a freshly created table.</summary>
            short sometimesNumFields;

            /// <summary>dosCodePage (uint16) @ 0x6A. Always 1252 (Windows-1252/ANSI) on every real BDE-created fixture examined.</summary>
            ushort dosCodePage;

            /// <summary>
            /// unknown6Cx6F (4 bytes) @ 0x6C-0x6F. For .DB files, bytes [0]
            /// and [1] are always 0x01, 0x01 on real BDE-created files - see
            /// class remarks; bytes [2] and [3] can be left zero.
            /// </summary>
            private byte[] unknown6Cx6F;  //array[$006C..$006F] of byte;

            /// <summary>changeCount4 (int16) @ 0x70. See <see cref="ParadoxHeaderOffsets.ChangeCount4"/> - the per-file write counter; 0 for a freshly created empty table.</summary>
            public short changeCount4;

            /// <summary>unknown72x77 (6 bytes) @ 0x72-0x77. 0 for a freshly created table.</summary>
            private byte[] unknown72x77; //    :  array[$0072..$0077] of byte;

            public V4Hdr(BinaryReader r)
            {
                fileVerID2 = r.ReadInt16();
                fileVerID3 = r.ReadInt16();
                encryption2 = r.ReadInt32();
                fileUpdateTime = r.ReadInt32(); // 4.0 only
                hiFieldID = r.ReadUInt16();
                hiFieldIDinfo = r.ReadUInt16();
                sometimesNumFields = r.ReadInt16();
                dosCodePage = r.ReadUInt16();
                unknown6Cx6F = r.ReadBytes(0x006F - 0x006C + 1); //array[$006C..$006F] of byte;
                changeCount4 = r.ReadInt16();
                unknown72x77 = r.ReadBytes(0x0077 - 0x0072 + 1); //    :  array[$0072..$0077] of byte;
            }

        }
    }
}
