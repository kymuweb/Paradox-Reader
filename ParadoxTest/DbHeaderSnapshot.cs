using System.Collections.Generic;
using System.IO;

namespace ParadoxTest
{
    /// <summary>
    /// Minimal, standalone decode of the well-understood leading .DB header
    /// fields, deliberately independent of ParadoxReader's own header parsing
    /// (see ParadoxReader.ParadoxFile.ReadHeader, whose offsets this
    /// intentionally mirrors) so it can be used as an out-of-band diagnostic
    /// cross-check when comparing table snapshots byte-for-byte - it does not
    /// touch/parse field defs.
    /// </summary>
    internal class DbHeaderSnapshot
    {
        public ushort RecordSize;
        public ushort HeaderSize;
        public byte   FileType;
        public byte   MaxTableSize;
        public int    RecordCount;
        public ushort NextBlock;
        public ushort FileBlocks;
        public ushort FirstBlock;
        public ushort LastBlock;
        public byte   ModifiedFlags1;
        public byte   ChangeCount1;
        public byte   ChangeCount2;
        public int    AutoIncVal;
        public byte   HasBlobFlag;

        public static DbHeaderSnapshot Read(string path)
        {
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var r = new BinaryReader(fs))
            {
                var h = new DbHeaderSnapshot();
                h.RecordSize   = r.ReadUInt16(); // 0x00
                h.HeaderSize   = r.ReadUInt16(); // 0x02
                h.FileType     = r.ReadByte();   // 0x04
                h.MaxTableSize = r.ReadByte();   // 0x05
                h.RecordCount  = r.ReadInt32();  // 0x06
                h.NextBlock    = r.ReadUInt16(); // 0x0A
                h.FileBlocks   = r.ReadUInt16(); // 0x0C
                h.FirstBlock   = r.ReadUInt16(); // 0x0E
                h.LastBlock    = r.ReadUInt16(); // 0x10
                r.ReadUInt16();                  // 0x12 unknown12x13
                h.ModifiedFlags1 = r.ReadByte(); // 0x14

                fs.Position = 0x2D;
                h.ChangeCount1 = r.ReadByte();    // 0x2D
                h.ChangeCount2 = r.ReadByte();    // 0x2E

                fs.Position = 0x49;
                h.AutoIncVal = r.ReadInt32();     // 0x49

                fs.Position = 0x74;
                h.HasBlobFlag = r.ReadByte();      // 0x74

                return h;
            }
        }

        public IEnumerable<string> DiffAgainst(DbHeaderSnapshot other)
        {
            if (RecordSize != other.RecordSize) yield return $"RecordSize: sr={RecordSize} h={other.RecordSize}";
            if (HeaderSize != other.HeaderSize) yield return $"HeaderSize: sr={HeaderSize} h={other.HeaderSize}";
            if (FileType != other.FileType) yield return $"FileType: sr={FileType} h={other.FileType}";
            if (MaxTableSize != other.MaxTableSize) yield return $"MaxTableSize: sr={MaxTableSize} h={other.MaxTableSize}";
            if (RecordCount != other.RecordCount) yield return $"RecordCount: sr={RecordCount} h={other.RecordCount}";
            if (NextBlock != other.NextBlock) yield return $"NextBlock: sr={NextBlock} h={other.NextBlock}";
            if (FileBlocks != other.FileBlocks) yield return $"FileBlocks: sr={FileBlocks} h={other.FileBlocks}";
            if (FirstBlock != other.FirstBlock) yield return $"FirstBlock: sr={FirstBlock} h={other.FirstBlock}";
            if (LastBlock != other.LastBlock) yield return $"LastBlock: sr={LastBlock} h={other.LastBlock}";
            if (ModifiedFlags1 != other.ModifiedFlags1) yield return $"ModifiedFlags1: sr=0x{ModifiedFlags1:X2} h=0x{other.ModifiedFlags1:X2}";
            if (ChangeCount1 != other.ChangeCount1) yield return $"ChangeCount1: sr={ChangeCount1} h={other.ChangeCount1}";
            if (ChangeCount2 != other.ChangeCount2) yield return $"ChangeCount2: sr={ChangeCount2} h={other.ChangeCount2}";
            if (AutoIncVal != other.AutoIncVal) yield return $"AutoIncVal: sr={AutoIncVal} h={other.AutoIncVal}";
            if (HasBlobFlag != other.HasBlobFlag) yield return $"HasBlobFlag: sr={HasBlobFlag} h={other.HasBlobFlag}";
        }
    }
}
