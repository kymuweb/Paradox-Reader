using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    public static class ExtensionMethods
    {
        public static string EnsureEndsWith(this string tableName, string suffix)
        {
            return (tableName?.EndsWith(suffix, StringComparison.OrdinalIgnoreCase) ?? false) ? tableName : (tableName + suffix);
        }
    }
}
