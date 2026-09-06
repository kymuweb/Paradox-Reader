using System;
using System.IO;
using System.Linq;

namespace ParadoxReader
{
    public partial class ParadoxFile
    {
        internal class DataBlock
        {
            public ParadoxFile file;
            ushort nextBlock;
            internal ushort blockNumber;
            short addDataSize;
            public byte[] data;
            private ParadoxReader.ParadoxRecord[] recCache;

            public int RecordCount { get; private set; }

            public DataBlock(ParadoxFile file, BinaryReader reader, ushort? expectedBlockNumber = null)
            {
                this.file = file;
                this.nextBlock = reader.ReadUInt16();
                this.blockNumber = reader.ReadUInt16();
                this.addDataSize = reader.ReadInt16();

                // Some real-world files (observed after a BDE "rebuild") leave stale
                // garbage in a block's own header (blockNumber/addDataSize) even
                // though the block's on-disk position (and expectedBlockNumber,
                // derived from that position) is authoritative. Previously this threw
                // an unhandled exception that aborted the whole table; self-heal by
                // trusting the position-derived value instead, and clamp the record
                // count so a corrupt addDataSize can't read past the block's actual
                // capacity (which would otherwise surface later as an
                // EndOfStreamException).
                if (expectedBlockNumber.HasValue && this.blockNumber != expectedBlockNumber)
                {
                    System.Diagnostics.Debug.WriteLine(
                        $"[DataBlock] Block header mismatch in '{file.TableName}': expected block number " +
                        $"{expectedBlockNumber} but got {this.blockNumber}. Trusting on-disk position.");
                    this.blockNumber = expectedBlockNumber.Value;
                }

                var recordCount = (addDataSize / (this.file.RecordSize)) + 1;
                int maxRecordsInBlock = Math.Max(0, (this.file.maxTableSize * 0x0400 - 6) / this.file.RecordSize);
                if (recordCount < 0 || recordCount > maxRecordsInBlock)
                {
                    System.Diagnostics.Debug.WriteLine(
                        $"[DataBlock] Implausible record count ({recordCount}) from addDataSize={addDataSize} " +
                        $"in '{file.TableName}' block {this.blockNumber}; clamping to 0.");
                    recordCount = 0;
                }
                this.RecordCount = recordCount;
                var recordCountBySize = this.RecordCount * (this.file.RecordSize);
                this.data = reader.ReadBytes(recordCountBySize);
                this.recCache = new ParadoxReader.ParadoxRecord[this.data.Length];
            }


            public ParadoxReader.ParadoxRecord this[int recIndex]
            {
                get
                {
                    if (this.recCache[recIndex] == null)
                    {
                        this.recCache[recIndex] = new ParadoxReader.ParadoxRecord(this, recIndex);
                    }
                    return this.recCache[recIndex];
                }
            }

            internal void WriteRecordToFile(int recIndex)
            {
                file.WriteRecords(this.data, this.blockNumber, new[] { recIndex } );
            }

            internal void WriteRecordsToFile()
            {
                file.WriteRecords(this.data, this.blockNumber, Enumerable.Range(0, this.data.Length).ToArray());
            }
        }
    }
}
