using System;
using System.Collections.Generic;
using System.Linq;

namespace ParadoxReader
{
    /// <summary>
    /// Describes a single field of a table schema being created or edited via
    /// <see cref="TableStructureDefinition"/>. This is a UI/edit-time model,
    /// distinct from the internal <see cref="ParadoxFile.FieldInfo"/> used by
    /// the low-level reader/writer, so it can be freely mutated (renamed,
    /// reordered, retyped) before being committed to disk.
    /// </summary>
    public class TableFieldDefinition
    {
        public string Name { get; set; }
        public ParadoxFieldTypes Type { get; set; }
        public byte Size { get; set; }

        /// <summary>
        /// True if this field is (part of) the primary key. Per Paradox
        /// convention (and this app's UI), primary-key fields are always
        /// the leading N fields of the table in declaration order - there
        /// is no independent ordering of key fields.
        /// </summary>
        public bool IsPrimaryKey { get; set; }

        public TableFieldDefinition()
        {
        }

        public TableFieldDefinition(string name, ParadoxFieldTypes type, byte size, bool isPrimaryKey)
        {
            Name = name;
            Type = type;
            Size = size;
            IsPrimaryKey = isPrimaryKey;
        }

        public TableFieldDefinition Clone()
        {
            return new TableFieldDefinition(Name, Type, Size, IsPrimaryKey);
        }

        /// <summary>
        /// Creates a blob field definition from a user-facing "leader size"
        /// (the number of bytes that can be stored directly inline in the .DB
        /// record before the value must be externalized to the .MB file) -
        /// mirroring how SQL's <c>BLOB(n, ...)</c> DDL syntax and the BDE UI
        /// both express blob field sizes, and how a table designer would
        /// naturally think of "how much can I write directly into the record".
        /// Memo is simply a blob sub-type (MemoBLOb/FmtMemoBLOb), so this same
        /// factory covers both memo and non-memo blob fields.
        ///
        /// On disk, a blob field's fSize is actually <paramref name="leaderSize"/>
        /// + 10 (the trailing 10 bytes are the blob pointer: offset+index (4),
        /// size (4), mod_nr (2) - see ParadoxBlobFile/ParadoxRecord for details).
        /// This factory hides that +10 adjustment so callers never need to
        /// reason about the on-disk leader/pointer split themselves.
        /// </summary>
        /// <param name="name">Field name.</param>
        /// <param name="type">Must be a memo/blob-capable type (MemoBLOb, FmtMemoBLOb, BLOb, OLE, or Graphic).</param>
        /// <param name="leaderSize">
        /// The inline byte capacity, matching the "n" in SQL's BLOB(n, ...) syntax.
        /// Must leave room for the 10-byte pointer, i.e. leaderSize + 10 &lt;= 255.
        /// Note: per Embarcadero/BDE documentation, Paradox BLOB(length, type) columns
        /// are only officially documented to support length between 0 and 240 (unlike
        /// dBASE tables, which allow up to 32,767). The reason for the 240 cap - rather
        /// than the 245 that would otherwise fit the 10-byte pointer within a byte-sized
        /// fSize - is not clear from available documentation/reference captures, so it is
        /// not enforced here; callers should be aware real BDE tooling may reject or
        /// behave differently for leaderSize values between 241 and 245.
        /// </param>
        /// <param name="isPrimaryKey">True if this field is (part of) the primary key.</param>
        public static TableFieldDefinition CreateBlobField(string name, ParadoxFieldTypes type, int leaderSize, bool isPrimaryKey = false)
        {
            if (leaderSize < 0 || leaderSize + 10 > 255)
                throw new ArgumentOutOfRangeException(nameof(leaderSize), leaderSize, "leaderSize must be between 0 and 245 (leaderSize + 10 must fit in a byte).");

            return new TableFieldDefinition(name, type, (byte)(leaderSize + 10), isPrimaryKey);
        }

        /// <summary>
        /// True if <paramref name="type"/> is one of the blob-capable field types
        /// (MemoBLOb, FmtMemoBLOb, BLOb, OLE, or Graphic), whose on-disk fSize
        /// includes a 10-byte pointer in addition to the user-facing leader size.
        /// </summary>
        public static bool IsBlobType(ParadoxFieldTypes type)
        {
            return type == ParadoxFieldTypes.MemoBLOb
                || type == ParadoxFieldTypes.FmtMemoBLOb
                || type == ParadoxFieldTypes.BLOb
                || type == ParadoxFieldTypes.OLE
                || type == ParadoxFieldTypes.Graphic;
        }
    }

    /// <summary>
    /// Describes one secondary index (a single logical .Xnn/.Xgn index, plus
    /// its .Ynn/.Ygn "maintained field" companion) to be created for a table,
    /// in terms of the (ordered) fields it indexes.
    /// </summary>
    public class TableIndexDefinition
    {
        /// <summary>
        /// Ordered list of field indices (into the owning
        /// <see cref="TableSchemaDefinition.Fields"/> list) that make up this
        /// index, in the order they should be indexed on.
        /// </summary>
        public List<int> FieldIndices { get; set; } = new List<int>();

        public TableIndexDefinition()
        {
        }

        public TableIndexDefinition(IEnumerable<int> fieldIndices)
        {
            FieldIndices = new List<int>(fieldIndices);
        }
    }

    /// <summary>
    /// Editable, in-memory description of a Paradox table's structure: its
    /// fields (name/type/size/primary-key flag) and secondary indexes. Used
    /// by the Table Structure UI (create/modify modes) and by
    /// <see cref="TableCreator"/>/<see cref="TableRebuilder"/> to build or
    /// regenerate the on-disk files.
    /// </summary>
    public class TableSchemaDefinition
    {
        public string TableName { get; set; }

        /// <summary>
        /// All fields in physical order. Primary-key fields must be the
        /// leading entries of this list (see <see cref="TableFieldDefinition.IsPrimaryKey"/>).
        /// </summary>
        public List<TableFieldDefinition> Fields { get; set; } = new List<TableFieldDefinition>();

        public List<TableIndexDefinition> Indexes { get; set; } = new List<TableIndexDefinition>();

        public int PrimaryKeyFieldCount => Fields.Count(f => f.IsPrimaryKey);

        /// <summary>
        /// Re-sorts <see cref="Fields"/> so all primary-key fields come first,
        /// preserving relative order within each group. Should be called
        /// whenever a field's <see cref="TableFieldDefinition.IsPrimaryKey"/>
        /// flag changes.
        /// </summary>
        public void ReorderPrimaryKeyFieldsFirst()
        {
            var pk = Fields.Where(f => f.IsPrimaryKey).ToList();
            var rest = Fields.Where(f => !f.IsPrimaryKey).ToList();
            Fields = pk.Concat(rest).ToList();
        }

        /// <summary>
        /// Builds a <see cref="TableSchemaDefinition"/> snapshot of an
        /// already-open table's current structure, suitable as the starting
        /// point for "Modify Structure" editing.
        /// </summary>
        public static TableSchemaDefinition FromTable(ParadoxTableFile table)
        {
            var schema = new TableSchemaDefinition
            {
                TableName = table.TableName
            };

            int primaryKeyCount = table.primaryKeyFields;
            for (int i = 0; i < table.FieldNames.Length; i++)
            {
                var field = table.FieldTypes[i];
                schema.Fields.Add(new TableFieldDefinition(
                    table.FieldNames[i], field.fType, field.fSize, i < primaryKeyCount));
            }

            foreach (var index in table.SecondaryIndexes)
            {
                schema.Indexes.Add(new TableIndexDefinition(index.FieldIndices));
            }

            return schema;
        }
    }
}
