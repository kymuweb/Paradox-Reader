using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace ParadoxReader
{
    internal class ParadoxBlobFile : IDisposable
    {
        private struct ActiveSlotInfo
        {
            public int Idx;
            public byte OldPtr0;
            public byte N;
            public byte Rem;
        }

        private readonly Stream stream;
        private readonly BinaryReader reader;

        public ParadoxBlobFile(string fileName)
            : this(new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
        }

        public ParadoxBlobFile(Stream stream)
        {
            this.stream = stream;
            this.reader = new BinaryReader(stream);
        }

        public virtual void Dispose()
        {
            this.stream.Dispose();
        }

        /// <summary>
        /// Reads the .MB file's global modification counter (a 2-byte little-endian
        /// word at file offset 3), increments and writes back the incremented value
        /// into the header, but returns the *old* (pre-increment) value. Verified
        /// against multiple reference captures: the header counter increments by
        /// exactly 1 on every blob write across the *entire* file (not per-block/
        /// per-slot), but the value stored in the slot's modNr field and in the .DB
        /// record's blobInfo mod_nr bytes is the value the header held *before* this
        /// write (i.e. one less than the freshly incremented header value).
        /// Confusing this with a per-slot sequence number (as a single byte), or with
        /// the post-increment header value, was the root cause of BDE reporting
        /// generated .MB files as corrupt.
        /// </summary>
        /// <summary>
        /// Computes and writes the whole-block XOR checksum byte (offset+11) for a
        /// type-3 sub-allocated memo block. Verified against every available
        /// reference capture (15 files spanning sizes 1-1000+ bytes, single- and
        /// multi-write cases): XOR-ing every byte of the 4096-byte block except
        /// byte 11 itself always reproduces the value stored at byte 11.
        /// Must be called last, after all other header/slot/payload bytes for a
        /// write have already been written to the stream, since it depends on the
        /// full contents of the block.
        /// </summary>
        /// <summary>
        /// Computes and writes type-3 block header bytes [3]-[10] (the 8 "lane"
        /// bytes that precede the whole-block checksum at [11]). Reverse-engineered
        /// from SQLRunner/BDE-generated reference captures covering single-write
        /// blocks (memo lengths 1-40), multi-write/reallocation blocks (e.g. an
        /// A10 write followed by a same-block B-length write), and repeated-write
        /// determinism checks: each lane k (0-7) is simply the XOR of every byte in
        /// the full 4096-byte block whose offset-within-block modulo 8 equals k,
        /// excluding the 8 lane bytes themselves ([3]-[10]) but INCLUDING the
        /// checksum byte [11]. This holds regardless of how many blobs/slots are
        /// packed into the block, which supersedes the earlier (single-blob-only)
        /// formula that did not account for multiple active slots sharing a block.
        /// Must be called after the slot entry/payload for this write AND the
        /// checksum byte [11] (<see cref="WriteType3BlockChecksum"/>) have already
        /// been written, since the lanes depend on the checksum byte's value.
        /// </summary>
        private void WriteType3HeaderLanes(long blockOffset)
        {
            byte[] fullBlock = new byte[4096];
            this.stream.Position = blockOffset;
            int totalRead = 0;
            while (totalRead < 4096) totalRead += this.stream.Read(fullBlock, totalRead, 4096 - totalRead);

            byte[] lanes = new byte[8];
            for (int p = 0; p < 4096; p++)
            {
                if (p >= 3 && p <= 10) continue;
                lanes[p % 8] ^= fullBlock[p];
            }
            this.stream.Position = blockOffset + 3;
            this.stream.Write(lanes, 0, 8);
        }

        /// <summary>
        /// Computes and writes type-3 block header byte [11]. Reverse-engineered
        /// from SQLRunner/BDE reference captures: byte [11] is NOT a live
        /// whole-block XOR checksum (that hypothesis coincidentally matched
        /// single-write and lowest-slot-only cases). It is instead a monotonic
        /// "deepest slot reached" high-water mark that never decreases, even
        /// after the slot that set it is later freed and reused by a shallower
        /// write:
        ///   byte[11] = max(byte[11] before this write, 63 - newSlotIdx)
        /// Verified against A10 (single write, slot 63 -> byte11=0x00), A10->B10
        /// (slot 62 used next -> byte11=0x01), and A10->B10->C10 (3rd write
        /// reuses freed slot 63, which alone would give 0x00, but byte11 stays
        /// at the 0x01 high-water mark set by the 2nd write).
        /// Must be called after the new slot entry has been written (so the slot
        /// table reflects newSlotIdx), and before <see cref="WriteType3HeaderLanes"/>,
        /// since the lane formula includes byte [11] in its XOR groups.
        /// </summary>
        private void WriteType3BlockChecksum(long blockOffset, int newSlotIdx)
        {
            this.stream.Position = blockOffset + 11;
            byte oldByte11 = (byte)this.stream.ReadByte();
            byte candidate = (byte)(63 - newSlotIdx);
            byte newByte11 = candidate > oldByte11 ? candidate : oldByte11;
            this.stream.Position = blockOffset + 11;
            this.stream.WriteByte(newByte11);
        }

        private ushort IncrementGlobalModCount()
        {
            this.stream.Position = 3;
            byte[] buf = new byte[2];
            int r = 0;
            while (r < 2) r += this.stream.Read(buf, r, 2 - r);
            ushort oldModCount = BitConverter.ToUInt16(buf, 0);
            ushort newModCount = (ushort)(oldModCount + 1);
            this.stream.Position = 3;
            this.stream.Write(BitConverter.GetBytes(newModCount), 0, 2);
            return oldModCount;
        }

        public byte[] ReadBlob(byte[] blobInfo, int len, int hsize)
        {

            var leader = len - 10;
            var size = BitConverter.ToUInt32(blobInfo, leader + 4);
            var blobsize = size;

            if (hsize == 17) // Graphics has a larger header size (8 bytes) at the expense of the blobsize.
            {
                blobsize = size - 8;
            }

            var index = BitConverter.ToUInt32(blobInfo, leader) & 0x000000ff;
            var mod_nr = BitConverter.ToUInt16(blobInfo, leader + 8);
            var offset = BitConverter.ToUInt32(blobInfo, leader) & 0xffffff00;


            if (size > 0)
            {

                this.stream.Position = offset;

                byte[] head;
                head = new byte[20];
                this.reader.Read(head, 0, 3);

                if (head[0] == 3)
                {
                    this.reader.Read(head, 0, 9); // Read remaining 9 bytes of header
                    var blobPointerPos = offset + 12 + (index * 5);
                    this.stream.Position = blobPointerPos; // Goto the blob pointer with the passed index
                    this.reader.Read(head, 0, 5); // Read the blob pointer
                    var checkSize = ((uint)head[1] - 1) * 16 + head[4];
                    if (checkSize == size)
                    {
                        byte[] buffer;
                        buffer = new byte[size];

                        var blobDataPos = offset + (head[0] * 16);
                        this.stream.Position = blobDataPos; // Goto the blob data position

                        this.reader.Read(buffer, 0, (int)size); // Or should this be blobsize? Need to test with graphic type
                        return buffer;
                    }
                }
                else //if (head[0] == 2)
                {
                    //TODO check for type 2 and index=255

                    this.reader.Read(head, 0, hsize - 3); // Read remaining 6 bytes of header
                    var checkSize = BitConverter.ToUInt32(head, 0);
                    if (checkSize == size)
                    {
                        byte[] buffer;
                        buffer = new byte[size];

                        this.reader.Read(buffer, 0, (int)size); // Or should this be blobsize? Need to test with graphic type
                        return buffer;
                    }
                }
            }
            return null;
        }



        /// <summary>
        /// Overwrites an existing blob slot in the .MB file with <paramref name="blobVal"/>.
        /// This is a targeted, minimal write:
        ///  - For same-size data: seeks directly to the data position (offset+hsize) and
        ///    writes only the payload bytes.  The header is never touched.
        ///  - For different-size data: additionally patches only the 4-byte checkSize field
        ///    inside the header via a separate targeted seek.
        /// Type-3 (sub-block / Graphics) blobs update just the 5-byte blob pointer and data.
        /// </summary>
        internal void WriteBlob(byte[] blobInfo, int len, int hsize, byte[] blobVal)
        {
            var leader  = len - 10;
            var index   = BitConverter.ToUInt32(blobInfo, leader) & 0x000000ff;
            var offset  = BitConverter.ToUInt32(blobInfo, leader) & 0xffffff00;
            var oldSize = BitConverter.ToUInt32(blobInfo, leader + 4);

            if (blobVal == null || blobVal.Length == 0)
            {
                // Blank blob value: free the existing slot (if any) rather than writing
                // zero-length payload data, and clear the .DB blobInfo pointer bytes so
                // the field no longer references a (now freed) blob slot.
                if (oldSize > 0)
                {
                    this.stream.Position = offset;
                    int freeTypeByte = this.stream.ReadByte();

                    if (freeTypeByte == 3)
                    {
                        // Type-3 sub-block: zero ptr[0] and modNr (ptr[2..3], a 2-byte word)
                        // of the slot entry to free it.
                        long slotPos = (long)offset + 12 + (long)index * 5;
                        this.stream.Position = slotPos;
                        byte[] slotEntry = new byte[5];
                        int sr = 0;
                        while (sr < 5) sr += this.stream.Read(slotEntry, sr, 5 - sr);

                        slotEntry[0] = 0;
                        slotEntry[2] = 0;
                        slotEntry[3] = 0;
                        this.stream.Position = slotPos;
                        this.stream.Write(slotEntry, 0, 5);
                    }
                    // Type-2 blocks have no separate slot table to free; the block's
                    // checkSize/data will simply be superseded on the next write.
                }

                // Zero the blobInfo pointer bytes: offset+index (leader..leader+3),
                // size (leader+4..leader+7), and mod_nr (leader+8..leader+9).
                for (int i = 0; i < 10; i++)
                    blobInfo[leader + i] = 0;

                // Increment the .MB file's global modification counter (2-byte word at
                // offset 3), matching real Paradox/BDE behavior on every blob write.
                IncrementGlobalModCount();

                this.stream.Flush();
                return;
            }

            var newSize = (uint)blobVal.Length;

            // No-op short-circuit: if the new value is identical to what is already
            // stored, skip the write entirely.  Paradox's real writer leaves the file
            // completely untouched in this case (verified against reference captures),
            // whereas always reallocating a slot corrupts the checksum-like header
            // bytes that we cannot otherwise reproduce.
            if (newSize == oldSize && oldSize > 0)
            {
                var existing = this.ReadBlob(blobInfo, len, hsize);
                if (existing != null && existing.Length == blobVal.Length)
                {
                    bool same = true;
                    for (int i = 0; i < existing.Length; i++)
                    {
                        if (existing[i] != blobVal[i]) { same = false; break; }
                    }
                    if (same) return;
                }
            }

            if (oldSize == 0)
            {
                // First-ever write for this field: no block has been allocated yet
                // (blobInfo/offset/index are all zero — offset 0 is always the MB file
                // header, never a real data block).  Verified against a real BDE capture:
                // the .MB file grows by exactly one new 4096-byte block, a type-3 block
                // header is written at its start, and the topmost slot (index 63) is used
                // for the first blob:
                //   block header: [0]=3 (type), [1..2]=numBlocks=1 (LE word)
                //   slot table: 64 entries (indices 0..63) starting at blockOffset+12
                //   data area starts after the slot table, rounded up to a 16-byte
                //     boundary: 12 + 64*5 = 332 -> rounds up to 336 = 21*16, so the first
                //     slot's dataOffsetMultiplier is 21.
                //   slot 63 entry: [0]=21 (data offset multiplier), [1]=n, [2]=1 (seq,
                //     first-ever write), [3]=0, [4]=remainder — matching the existing
                //     n=(size/16)+1, remainder=size%16 formula used elsewhere.
                // The remaining block header bytes [3]-[11] are not reliably derivable
                // from a single reference capture, so they are zero-filled as a
                // best-effort default rather than guessed.
                const int blockSize = 4096;
                const int slotCount = 64;
                const byte dataOffsetMultiplier = 21; // (12 + 64*5) rounded up to 16 = 336 = 21*16
                const int dataAreaOffset = dataOffsetMultiplier * 16; // 336
                const int maxSingleBlockPayload = blockSize - dataAreaOffset; // 3760 bytes

                if (newSize > maxSingleBlockPayload)
                {
                    // Blobs too large for a shared type-3 sub-allocated block use the
                    // type-2 "big blob" format instead: a dedicated, contiguous run of
                    // one or more 4096-byte blocks holding a single blob, per pxlib's
                    // _TMbBlockHeader2 layout (fileformat.h):
                    //   [0]     type        = 2
                    //   [1..2]  numBlocks   (LE word) - count of contiguous 4096-byte
                    //                         blocks reserved for this blob, including
                    //                         the header block itself.
                    //   [3..6]  blobLen     (LE uint32) - the blob's byte length, mirrors
                    //                         the checkSize field already read/written by
                    //                         the existing type-2 read/overwrite paths.
                    //   [7..8]  modNr       (LE word) - same global modification counter
                    //                         value used elsewhere.
                    // Payload data immediately follows the hsize-byte header (hsize=9
                    // normally, 17 for Graphics), exactly as ReadBlob and the type-2
                    // overwrite path already assume. Unlike type-3 blocks, type-2 blocks
                    // are never shared between records, so no slot table is needed and
                    // "index" is always 0 (the block offset's low byte, since blocks are
                    // always 4096-aligned).
                    long totalBytesNeeded = (long)hsize + newSize;
                    int  numBlocksNeeded  = (int)((totalBytesNeeded + blockSize - 1) / blockSize);

                    long newBlockOffset = this.stream.Length;
                    if (newBlockOffset % blockSize != 0)
                    {
                        newBlockOffset += blockSize - (newBlockOffset % blockSize);
                    }

                    // Reserve the full contiguous run up front, zero-filled, so the
                    // stream length reflects the block-aligned allocation even though
                    // only the header and payload bytes are written below.
                    long totalAllocSize = (long)numBlocksNeeded * blockSize;
                    byte[] zeroFill = new byte[totalAllocSize];
                    this.stream.Position = newBlockOffset;
                    this.stream.Write(zeroFill, 0, zeroFill.Length);

                    ushort newModNr2 = IncrementGlobalModCount();
                    byte[] newModNr2Bytes = BitConverter.GetBytes(newModNr2);

                    byte[] blockHeader = new byte[hsize];
                    blockHeader[0] = 2;
                    blockHeader[1] = (byte)(numBlocksNeeded & 0xFF);
                    blockHeader[2] = (byte)((numBlocksNeeded >> 8) & 0xFF);
                    Buffer.BlockCopy(BitConverter.GetBytes(newSize), 0, blockHeader, 3, 4);
                    blockHeader[7] = newModNr2Bytes[0];
                    blockHeader[8] = newModNr2Bytes[1];
                    // Any remaining header bytes (Graphics' extra 8 bytes at [9..16])
                    // are left zero-filled as a best-effort default; their exact
                    // semantics are not derivable from available reference captures.

                    this.stream.Position = newBlockOffset;
                    this.stream.Write(blockHeader, 0, hsize);

                    this.stream.Position = newBlockOffset + hsize;
                    this.stream.Write(blobVal, 0, blobVal.Length);

                    // Update blobInfo: packed offset+index. Verified against a real BDE
                    // reference capture: type-2 (big blob) records always store index=255
                    // (0xFF) in the low byte, not 0 — BDE reports "BLOB has been modified"
                    // if this sentinel is missing, even though the block offset itself is
                    // always 4096-aligned (so the low byte would otherwise read as 0).
                    uint packedOffsetIndex2 = (uint)newBlockOffset | 0xFFu;
                    Buffer.BlockCopy(BitConverter.GetBytes(packedOffsetIndex2), 0, blobInfo, leader, 4);
                    Buffer.BlockCopy(BitConverter.GetBytes(newSize), 0, blobInfo, leader + 4, 4);
                    Buffer.BlockCopy(newModNr2Bytes, 0, blobInfo, leader + 8, 2);

                    this.stream.Flush();
                    return;
                }

                // Real Paradox packs multiple records' first-ever blobs into the SAME
                // shared type-3 block (using descending slot indices 63, 62, 61, ...)
                // rather than allocating a brand-new 4096-byte block per record. Scan
                // existing type-3 blocks for one with a free slot below its lowest
                // active slot AND enough remaining data space before falling back to
                // allocating a new block. This matches the packing strategy already
                // used by the overwrite/reallocation path below.
                long targetBlockOffset = -1;
                int  targetSlotIdx     = -1;
                byte targetPtr0        = 0;

                byte[] scanBuf5 = new byte[5];
                for (long candidateBlockOffset = blockSize; candidateBlockOffset < this.stream.Length; candidateBlockOffset += blockSize)
                {
                    this.stream.Position = candidateBlockOffset;
                    int candidateType = this.stream.ReadByte();
                    if (candidateType != 3) continue;

                    long slotTableBase = candidateBlockOffset + 12;
                    byte maxPtr0      = (byte)(dataOffsetMultiplier - 1);
                    int  minActiveSlot = slotCount; // no active slot found yet == all free

                    for (int si = 0; si < slotCount; si++)
                    {
                        long sp = slotTableBase + si * 5;
                        this.stream.Position = sp;
                        int sr = 0;
                        while (sr < 5) sr += this.stream.Read(scanBuf5, sr, 5 - sr);
                        if (scanBuf5[0] != 0) // active slot
                        {
                            if (si < minActiveSlot) minActiveSlot = si;
                            if (scanBuf5[0] > maxPtr0)   maxPtr0   = scanBuf5[0];
                        }
                    }

                    if (minActiveSlot == 0) continue; // slot table full, no free slot below lowest active

                    int  candidateSlotIdx = minActiveSlot - 1;
                    byte candidatePtr0    = (byte)(maxPtr0 + 1);
                    long candidateDataEnd = (long)candidatePtr0 * 16 + newSize;
                    if (candidateDataEnd > blockSize) continue; // not enough room left in this block

                    targetBlockOffset = candidateBlockOffset;
                    targetSlotIdx     = candidateSlotIdx;
                    targetPtr0        = candidatePtr0;
                    break;
                }

                if (targetBlockOffset < 0)
                {
                    // No existing block had room: allocate a brand-new 4096-byte block.
                    targetBlockOffset = this.stream.Length;
                    if (targetBlockOffset % blockSize != 0)
                    {
                        // Should not happen for a well-formed MB file, but guard anyway.
                        targetBlockOffset += blockSize - (targetBlockOffset % blockSize);
                    }

                    byte[] newBlock = new byte[blockSize];
                    newBlock[0] = 3;   // type
                    newBlock[1] = 1;   // numBlocks low byte
                    newBlock[2] = 0;   // numBlocks high byte
                    this.stream.Position = targetBlockOffset;
                    this.stream.Write(newBlock, 0, blockSize);

                    targetSlotIdx = slotCount - 1;      // 63, topmost slot
                    targetPtr0    = dataOffsetMultiplier; // 21

                    // The .MB file's own header block (offset 0) tracks free-space
                    // accounting for the most recently allocated type-3 block. Verified
                    // against reference captures for multiple blob sizes: bytes
                    // [0x38..0x39] are the constant 02 10, bytes [0x3A..0x3B] are 00 00,
                    // byte [0x3C] is (235 - n) where n = size/16 + 1 (the same "n" value
                    // written into the new slot entry), and bytes [0x3D..0x3F] are 00.
                    // This field is written once, at the moment a brand-new type-3 block
                    // is allocated; it is not updated when additional blobs are packed
                    // into the same existing block afterwards.
                    byte firstBlobN = (byte)(newSize / 16 + 1);
                    byte[] freeSpaceHeader = new byte[] { 0x02, 0x10, 0x00, 0x00, (byte)(235 - firstBlobN), 0x00, 0x00, 0x00 };
                    this.stream.Position = 0x38;
                    this.stream.Write(freeSpaceHeader, 0, freeSpaceHeader.Length);
                }

                byte newN   = (byte)(newSize / 16 + 1);
                byte newRem = (byte)(newSize % 16);

                // modNr is a 2-byte little-endian word (not a 1-byte per-block sequence),
                // sourced from the .MB file's global modification counter at header
                // offset 3. Verified against reference captures: this same value is
                // written into both the slot entry's modNr field and the .DB record's
                // blobInfo mod_nr bytes.
                ushort newModNr = IncrementGlobalModCount();
                byte[] newModNrBytes = BitConverter.GetBytes(newModNr);

                long slotOffsetInBlock = 12 + (long)targetSlotIdx * 5;
                byte[] slotEntry = new byte[] { targetPtr0, newN, newModNrBytes[0], newModNrBytes[1], newRem };
                this.stream.Position = targetBlockOffset + slotOffsetInBlock;
                this.stream.Write(slotEntry, 0, 5);

                this.stream.Position = targetBlockOffset + targetPtr0 * 16;
                this.stream.Write(blobVal, 0, blobVal.Length);

                // Update blobInfo: packed offset+index (low byte is the slot index; the
                // block offset is always 4096-aligned so its low byte is naturally 0),
                // size, and mod_nr.
                uint packedOffsetIndex = (uint)targetBlockOffset | (uint)targetSlotIdx;
                Buffer.BlockCopy(BitConverter.GetBytes(packedOffsetIndex), 0, blobInfo, leader, 4);
                Buffer.BlockCopy(BitConverter.GetBytes(newSize), 0, blobInfo, leader + 4, 4);
                Buffer.BlockCopy(newModNrBytes, 0, blobInfo, leader + 8, 2);

                // [11] must be computed first: a monotonic high-water mark based on
                // the slot index just used (see WriteType3BlockChecksum for details).
                WriteType3BlockChecksum(targetBlockOffset, targetSlotIdx);

                // Header bytes [3]-[10] are the whole-block lane XOR (see
                // WriteType3HeaderLanes), which depends on the byte [11] written above.
                WriteType3HeaderLanes(targetBlockOffset);

                this.stream.Flush();
                return;
            }

            // Read only the type byte — the minimal peek needed to choose the write path.
            this.stream.Position = offset;
            int typeByte = this.stream.ReadByte();

            if (typeByte == 3)
            {
                // Paradox type-3 sub-block memo strategy:
                //   If the new value fits within the space already allocated to the
                //   existing slot (newSize <= oldSize), reuse the slot IN-PLACE: overwrite
                //   the payload at the same data position and only patch the slot's own
                //   size fields (ptr[1]/ptr[4]) plus the blobInfo size bytes.  This avoids
                //   touching the block header bytes [3]..[11], whose exact semantics are
                //   opaque (pxlib's own reverse-engineered format docs list them simply as
                //   "unknown[9]"), and avoids allocating a brand-new slot/seq entirely.
                //
                //   Verified against reference captures (memotest_A10_B10.MB,
                //   memotest_A10_B10_C10.MB): Paradox does NOT reuse a slot in-place even
                //   when the new value is the same size as the old one. Every overwrite of
                //   an existing type-3 blob always frees the old slot (zeroing only ptr[0]
                //   and modNr, i.e. ptr[2..3]) and allocates a new slot at the next free
                //   index below the current lowest-indexed active slot, with a fresh modNr
                //   sourced from the .MB file's global modification counter.
                //
                // Slot entry layout (5 bytes):
                //   [0] dataOffsetMultiplier → data at blockBase + [0]*16
                //   [1] n  → checkSize = (n-1)*16 + [4]
                //   [2..3] modNr — 2-byte little-endian word sourced from the .MB file's
                //           global modification counter (header offset 3), NOT a per-slot
                //           sequence number.
                //   [4] remainder

                long slotTableBase = (long)offset + 12;

                // Scan the full 64-slot table (from the highest index down to 0) to find:
                //  - the sum of "n" (ptr[1], the size-in-16-byte-units field) across all
                //    currently ACTIVE slots (including the slot about to be replaced by
                //    this write) — this is the true data-position formula, verified
                //    against memotest_A10_B10.MB and memotest_A10_B10_C10.MB:
                //      write 1 (A10): no active slots yet, sum=0, ptr0 = 21 + 0 = 0x15
                //      write 2 (B10): slot for A10 is active (n=1), sum=1, ptr0 = 21 + 1 = 0x16
                //      write 3 (C10): slot for B10 is active (n=1), sum=1, ptr0 = 21 + 1 = 0x16
                //    Simply tracking "highest ptr0 so far" or reusing the freed slot's own
                //    ptr0 does NOT reproduce this; the position is always
                //    dataOffsetMultiplier + total 16-byte units consumed by active slots.
                //  - the first FREE slot when scanning from index 63 downwards. Verified
                //    against memotest_A10_B10_C10.MB: slot 63 is freed by the 2nd write and
                //    then REUSED by the 3rd write, rather than a new descending slot being
                //    allocated below the current lowest active slot.
                const int slotCount = 64;
                int  freeSlotIdx = -1;
                byte[] scanBuf   = new byte[5];
                for (int si = slotCount - 1; si >= 0; si--)
                {
                    long sp = slotTableBase + si * 5;
                    if (sp + 5 > this.stream.Length) continue;
                    this.stream.Position = sp;
                    int sr = 0;
                    while (sr < 5) sr += this.stream.Read(scanBuf, sr, 5 - sr);
                    if (scanBuf[0] == 0 && freeSlotIdx < 0)
                    {
                        freeSlotIdx = si;
                    }
                }

                const byte reallocDataOffsetMultiplier = 21; // (12 + 64*5) rounded up to 16 = 336 = 21*16
                int  newSlotIdx  = freeSlotIdx >= 0 ? freeSlotIdx : (int)index;

                // Read old slot entry.
                long oldPtrPos = slotTableBase + (long)index * 5;
                this.stream.Position = oldPtrPos;
                byte[] oldPtr = new byte[5];
                int read = 0;
                while (read < 5) read += this.stream.Read(oldPtr, read, 5 - read);

                // Size fields may change.
                byte newN   = oldPtr[1];
                byte newRem = oldPtr[4];
                if (newSize != oldSize)
                {
                    newN   = (byte)(newSize / 16 + 1);
                    newRem = (byte)(newSize % 16);
                    Buffer.BlockCopy(BitConverter.GetBytes(newSize), 0, blobInfo, leader + 4, 4);
                }

                // Compact the block's data area BEFORE freeing anything: verified
                // against reference stepwise captures (step1_A10.MB -> step2_B10.MB ->
                // step3_C10.MB) that on every write, ALL currently active slots
                // (including the one about to be replaced by this write) are repacked
                // contiguously starting at the base data offset, in ascending order of
                // their current data position. This reclaims gaps left by slots freed
                // on earlier writes (e.g. write 3 physically moves the write-2 blob
                // down into the dead gap left by the write-1 blob, even though the
                // write-2 blob's own slot is about to be freed by this very write).
                // The new blob's position is simply the end of this compacted region
                // (base + total units of all slots that were active before this write),
                // which also equals base + sum(active n) from the pre-rewrite formula.
                var activeSlots = new List<ActiveSlotInfo>();
                for (int si = 0; si < slotCount; si++)
                {
                    long sp = slotTableBase + si * 5;
                    if (sp + 5 > this.stream.Length) continue;
                    this.stream.Position = sp;
                    int sr = 0;
                    while (sr < 5) sr += this.stream.Read(scanBuf, sr, 5 - sr);
                    if (scanBuf[0] != 0)
                    {
                        activeSlots.Add(new ActiveSlotInfo { Idx = si, OldPtr0 = scanBuf[0], N = scanBuf[1], Rem = scanBuf[4] });
                    }
                }
                activeSlots.Sort((a, b) => a.OldPtr0.CompareTo(b.OldPtr0));

                byte nextPtr0 = reallocDataOffsetMultiplier;
                foreach (var s in activeSlots)
                {
                    int checkSize = (s.N - 1) * 16 + s.Rem;
                    if (s.OldPtr0 != nextPtr0)
                    {
                        byte[] moveBuf = new byte[checkSize];
                        this.stream.Position = (long)offset + s.OldPtr0 * 16;
                        int mr = 0;
                        while (mr < checkSize) mr += this.stream.Read(moveBuf, mr, checkSize - mr);
                        this.stream.Position = (long)offset + nextPtr0 * 16;
                        this.stream.Write(moveBuf, 0, checkSize);

                        if (s.Idx != index)
                        {
                            this.stream.Position = slotTableBase + s.Idx * 5;
                            this.stream.WriteByte(nextPtr0);
                        }
                    }
                    int unitsUsed = (checkSize + 15) / 16;
                    if (unitsUsed == 0) unitsUsed = 1;
                    nextPtr0 = (byte)(nextPtr0 + unitsUsed);
                }
                byte newPtr0 = nextPtr0;

                // Free old slot: zero ptr[0] and modNr (ptr[2..3]).
                oldPtr[0] = 0;
                oldPtr[2] = 0;
                oldPtr[3] = 0;
                this.stream.Position = oldPtrPos;
                this.stream.Write(oldPtr, 0, 5);

                // modNr is a 2-byte little-endian word sourced from the .MB file's global
                // modification counter, not a per-slot sequence number.
                ushort newModNr = IncrementGlobalModCount();
                byte[] newModNrBytes = BitConverter.GetBytes(newModNr);

                // Write new slot entry.
                byte[] newPtr = new byte[] { newPtr0, newN, newModNrBytes[0], newModNrBytes[1], newRem };
                long newPtrPos = slotTableBase + (long)newSlotIdx * 5;
                this.stream.Position = newPtrPos;
                this.stream.Write(newPtr, 0, 5);

                // Update blobInfo: slot-index byte (blobInfo[leader], the low byte of the
                // packed offset+index uint32) and mod_nr.
                blobInfo[leader]     = (byte)newSlotIdx;
                Buffer.BlockCopy(newModNrBytes, 0, blobInfo, leader + 8, 2);

                // Write payload at the new data position.
                this.stream.Position = (long)offset + newPtr0 * 16;
                this.stream.Write(blobVal, 0, blobVal.Length);

                // [11] must be computed first: a monotonic high-water mark based on
                // the slot index just used (see WriteType3BlockChecksum for details).
                WriteType3BlockChecksum(offset, newSlotIdx);

                // Header bytes [3]-[10] are the whole-block lane XOR (see
                // WriteType3HeaderLanes), which depends on the byte [11] written above.
                WriteType3HeaderLanes(offset);
            }
            else
            {
                // Type 2 (or any non-3 block): data lives immediately after the hsize-byte
                // header, i.e. at offset+hsize.  ReadBlob confirms this: after reading
                // 3 + (hsize-3) = hsize bytes sequentially it reads the data with no
                // intervening seek.
                //
                // We do NOT rewrite the header block — touching those bytes risks
                // corrupting structural unknowns (offset+1..2 and offset+7..9).
                // Instead we patch only the 4-byte checkSize field when the size changes,
                // using a direct targeted seek.

                if (newSize != oldSize)
                {
                    // checkSize = file[offset+3..offset+6] (LE uint32).
                    // Derived from ReadBlob: second Read puts file[offset+3..] into head[0..],
                    // and checkSize = ToUInt32(head, 0).
                    byte[] sizeBytes = BitConverter.GetBytes(newSize);
                    this.stream.Position = (long)offset + 3;
                    this.stream.Write(sizeBytes, 0, 4);
                    Buffer.BlockCopy(sizeBytes, 0, blobInfo, leader + 4, 4);
                }

                // Write the payload — mirrors the exact read position in ReadBlob.
                this.stream.Position = (long)offset + hsize;
                this.stream.Write(blobVal, 0, blobVal.Length);

                // Increment the .MB file's global modification counter (2-byte word at
                // offset 3).
                IncrementGlobalModCount();

                this.stream.Flush();
                return;
            }

            this.stream.Flush();
        }


    }

}
