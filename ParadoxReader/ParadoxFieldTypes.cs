using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    public enum ParadoxFieldTypes : byte
    {
        Alpha = 0x01,
        Date = 0x02,
        Short = 0x03,
        Long = 0x04,
        Currency = 0x05,
        Number = 0x06,
        Logical = 0x09,
        MemoBLOb = 0x0C,
        BLOb = 0x0D,
        FmtMemoBLOb = 0x0E,
        OLE = 0x0F,
        Graphic = 0x10,
        Time = 0x14,
        Timestamp = 0x15,
        AutoInc = 0x16,
        BCD = 0x17,
        Bytes = 0x18
    }

    public static class ParadoxFieldTypeSizes
    {
        /// <summary>
        /// Returns the fixed, non-negotiable on-disk size (in bytes) for
        /// <paramref name="type"/>, or null if the field's size is
        /// user-defined (Alpha, Bytes, BCD).
        /// </summary>
        public static byte? GetFixedSize(ParadoxFieldTypes type)
        {
            switch (type)
            {
                case ParadoxFieldTypes.Short:
                    return 2;
                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                    return 4;
                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    return 8;
                case ParadoxFieldTypes.Date:
                    return 4;
                case ParadoxFieldTypes.Time:
                    return 4;
                case ParadoxFieldTypes.Timestamp:
                    return 8;
                case ParadoxFieldTypes.Logical:
                    return 1;
                default:
                    // Alpha, Bytes, BCD, and Memo/BLOb/OLE/Graphic (whose "size" is a
                    // different, user-meaningful property - e.g. max in-place length -
                    // rather than a fixed on-disk reference width): user-defined size.
                    return null;
            }
        }
    }
}
