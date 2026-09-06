using System;

namespace ParadoxTest
{
    /// <summary>
    /// Small helpers for APIs that only exist in .NET 4.0+ but are convenient
    /// to have when targeting .NET Framework 3.5 (this project's target).
    /// </summary>
    internal static class Net35Compat
    {
        /// <summary>
        /// .NET 3.5-compatible replacement for the .NET 4.0+
        /// <c>Net35Compat.IsNullOrWhiteSpace(string)</c>.
        /// </summary>
        public static bool IsNullOrWhiteSpace(string value)
        {
            if (value == null) return true;
            for (int i = 0; i < value.Length; i++)
            {
                if (!char.IsWhiteSpace(value[i])) return false;
            }
            return true;
        }
    }
}
