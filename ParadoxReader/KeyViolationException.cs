using System;

namespace ParadoxReader
{
    /// <summary>
    /// Thrown before any write when a row would duplicate an existing key in a unique
    /// secondary index, mirroring BDE's "Key violation" error.
    /// </summary>
    public class KeyViolationException : Exception
    {
        public string IndexFilePath { get; }

        public KeyViolationException(string indexFilePath, string message)
            : base(message)
        {
            IndexFilePath = indexFilePath;
        }
    }
}
