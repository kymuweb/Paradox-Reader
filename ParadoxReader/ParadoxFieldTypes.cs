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
}
