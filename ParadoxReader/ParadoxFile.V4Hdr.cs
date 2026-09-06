using System.IO;

namespace ParadoxReader
{
    public partial class ParadoxFile
    {
        public class V4Hdr
        {
            short fileVerID2;
            short fileVerID3;
            internal int encryption2;
            public int Encryption2 => encryption2;
            int fileUpdateTime;  // 4.0 only
            ushort hiFieldID;
            ushort hiFieldIDinfo;
            short sometimesNumFields;
            ushort dosCodePage;
            private byte[] unknown6Cx6F;  //array[$006C..$006F] of byte;
            public short changeCount4;
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
