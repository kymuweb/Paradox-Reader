using System.IO;

namespace ParadoxReader
{
    public partial class ParadoxFile
    {
        internal class FieldInfo
        {
            public ParadoxFieldTypes fType;
            public byte fSize;

            public FieldInfo(ParadoxFieldTypes fType, byte fSize)
            {
                this.fType = fType;
                this.fSize = fSize;
            }

            public FieldInfo(BinaryReader r)
            {
                this.fType = (ParadoxFieldTypes)r.ReadByte();
                this.fSize = r.ReadByte();
            }
        }
    }
}
