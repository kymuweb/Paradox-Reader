using System;
using System.IO;

namespace ParadoxReader
{
    /// <summary>
    /// Transparently encrypts/decrypts the data blocks of a password-protected
    /// index file (.PX/.Xnn/.Ynn/.XGn/.YGn). BDE encrypts index blocks with the same
    /// cipher and key as .DB blocks (see <see cref="PxCrypt"/>), using the 1-based
    /// number of the block after the header. The header itself is never encrypted.
    /// </summary>
    internal sealed class BlockCryptStream : Stream
    {
        private readonly Stream inner;
        private readonly int headerSize;
        private readonly int blockSize;
        private readonly uint key;
        private long position;

        public BlockCryptStream(Stream inner, int headerSize, int blockSize, uint key)
        {
            this.inner = inner;
            this.headerSize = headerSize;
            this.blockSize = blockSize;
            this.key = key;
            position = inner.Position;
        }

        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => inner.CanSeek;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;
        public override long Position { get => position; set => position = value; }

        public override void Flush() => inner.Flush();
        public override void SetLength(long value) => inner.SetLength(value);

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:   position = offset; break;
                case SeekOrigin.Current: position += offset; break;
                case SeekOrigin.End:     position = inner.Length + offset; break;
            }
            return position;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int total = 0;
            while (count > 0 && position < inner.Length)
            {
                int n;
                if (position < headerSize)
                {
                    inner.Position = position;
                    n = inner.Read(buffer, offset, (int)Math.Min(count, headerSize - position));
                }
                else
                {
                    long index = (position - headerSize) / blockSize;
                    int within = (int)((position - headerSize) % blockSize);
                    var block = ReadPlainBlock(index);
                    long available = inner.Length - position;
                    n = (int)Math.Min(Math.Min(count, blockSize - within), available);
                    Buffer.BlockCopy(block, within, buffer, offset, n);
                }
                if (n <= 0) break;
                position += n; offset += n; count -= n; total += n;
            }
            return total;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                int n;
                if (position < headerSize)
                {
                    n = (int)Math.Min(count, headerSize - position);
                    inner.Position = position;
                    inner.Write(buffer, offset, n);
                }
                else
                {
                    long index = (position - headerSize) / blockSize;
                    int within = (int)((position - headerSize) % blockSize);
                    n = Math.Min(count, blockSize - within);
                    var block = ReadPlainBlock(index);
                    Buffer.BlockCopy(buffer, offset, block, within, n);
                    PxCrypt.EncryptDbBlock(block, 0, blockSize, key, (uint)(index + 1));
                    inner.Position = headerSize + index * blockSize;
                    inner.Write(block, 0, blockSize);
                }
                position += n; offset += n; count -= n;
            }
        }

        private byte[] ReadPlainBlock(long index)
        {
            var block = new byte[blockSize];
            long start = headerSize + index * blockSize;
            if (start >= inner.Length) return block;
            inner.Position = start;
            int read = 0, r;
            while (read < blockSize && (r = inner.Read(block, read, blockSize - read)) > 0) read += r;
            if (read == 0) return block;
            PxCrypt.DecryptDbBlock(block, 0, blockSize, key, (uint)(index + 1));
            return block;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
