using System;

namespace ParadoxReader
{
    /// <summary>
    /// Represents a memo (MemoBLOb / FmtMemoBLOb) field value.
    /// Bundles the decoded text with the raw blobInfo reference bytes that
    /// are stored in the .DB record and point to the slot in the .MB file.
    /// The blobInfo must be preserved across a read-modify-write cycle so
    /// that WriteBlob can locate and overwrite the correct slot.
    /// </summary>
    public sealed class MemoValue : ICloneable
    {
        /// <summary>Decoded text content of the memo field.</summary>
        public string Text { get; set; }

        /// <summary>
        /// Raw reference bytes (typically 10 bytes) stored in the .DB record.
        /// Encodes the offset and sub-index into the .MB blob file.
        /// </summary>
        public byte[] BlobInfo { get; }

        public MemoValue(string text, byte[] blobInfo)
        {
            Text     = text;
            BlobInfo = blobInfo;
        }

        public object Clone() =>
            new MemoValue(Text, BlobInfo != null ? (byte[])BlobInfo.Clone() : null);

        public override string ToString() => Text ?? string.Empty;
    }
}
