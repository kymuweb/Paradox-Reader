using System;
using System.IO;

namespace ParadoxReader
{
    /// <summary>
    /// Thin <see cref="Stream"/> wrapper that forwards every operation to an
    /// inner stream except <see cref="Close"/>/<see cref="Dispose(bool)"/>,
    /// which are no-ops. Used in place of the .NET 4.5+
    /// <c>new BinaryReader(stream, encoding, leaveOpen: true)</c> /
    /// <c>new BinaryWriter(stream, encoding, leaveOpen: true)</c> constructor
    /// overloads, which are not available when targeting .NET Framework 3.5.
    /// </summary>
    internal sealed class NonClosingStreamWrapper : Stream
    {
        private readonly Stream inner;

        public NonClosingStreamWrapper(Stream inner)
        {
            this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => inner.CanSeek;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;

        public override long Position
        {
            get => inner.Position;
            set => inner.Position = value;
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count)
            => inner.Read(buffer, offset, count);

        public override long Seek(long offset, SeekOrigin origin)
            => inner.Seek(offset, origin);

        public override void SetLength(long value) => inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count)
            => inner.Write(buffer, offset, count);

        public override void Close()
        {
            // Intentionally does not close/dispose the inner stream.
        }

        protected override void Dispose(bool disposing)
        {
            // Intentionally does not dispose the inner stream.
        }
    }
}
