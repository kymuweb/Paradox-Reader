using System;
using System.IO;
using System.Linq;

namespace ParadoxReader
{
    /// <summary>
    /// Creates brand-new, empty Paradox tables from a
    /// <see cref="TableSchemaDefinition"/> (used by File > New > Table).
    /// Writes a .DB file (and, if the schema has primary-key fields, an
    /// accompanying empty .PX file) built entirely from
    /// <see cref="ParadoxHeaderBuilder"/>, with zero records - there is
    /// nothing to migrate for a brand-new table, unlike
    /// <see cref="TableRebuilder.RebuildWithSchema"/>.
    /// </summary>
    public static class TableCreator
    {
        /// <summary>
        /// Creates a new, empty table at <paramref name="dbFilePath"/>
        /// matching <paramref name="schema"/>. The path must not already
        /// exist. Secondary indexes described in
        /// <see cref="TableSchemaDefinition.Indexes"/> are created as empty
        /// .Xnn files alongside the .DB (and its .PX, if any).
        /// </summary>
        public static void CreateNew(string dbFilePath, TableSchemaDefinition schema)
        {
            if (string.IsNullOrEmpty(dbFilePath)) throw new ArgumentNullException(nameof(dbFilePath));
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (schema.Fields == null || schema.Fields.Count == 0)
                throw new ArgumentException("A table must have at least one field.", nameof(schema));
            if (File.Exists(dbFilePath))
                throw new IOException($"A file already exists at '{dbFilePath}'.");

            if (string.IsNullOrEmpty(schema.TableName))
                schema.TableName = Path.GetFileNameWithoutExtension(dbFilePath);

            byte[] dbHeader = ParadoxHeaderBuilder.BuildDbHeader(schema);
            File.WriteAllBytes(dbFilePath, dbHeader);

            if (schema.PrimaryKeyFieldCount > 0)
            {
                string pxPath = Path.ChangeExtension(dbFilePath, ".PX");
                byte[] pxHeader = ParadoxHeaderBuilder.BuildPxHeader(schema);
                File.WriteAllBytes(pxPath, pxHeader);
            }

            // A .DB file with any memo/BLOb-typed field must have an
            // accompanying .MB blob file, even before any blob value has
            // ever been written - real BDE-created tables always create it
            // alongside the .DB at table-creation time (confirmed against
            // SQLRunner-created PKALPBLOB/PKALPMEMO/AUTOALPBLOB fixtures,
            // which all have a blank .MB present immediately after CREATE
            // TABLE). Without it, BDE apps report "File or directory does
            // not exist" for the .MB when opening the table.
            if (schema.Fields.Any(f => IsBlobFieldType(f.Type)))
            {
                string mbPath = Path.ChangeExtension(dbFilePath, ".MB");
                byte[] mbHeader = ParadoxHeaderBuilder.BuildBlankMbHeader();
                File.WriteAllBytes(mbPath, mbHeader);
            }

            // Every index this library creates corresponds to a named index
            // (equivalent to SQLRunner's CREATE INDEX), which real BDE always
            // names .XGn/.YGn with a sequential ordinal - confirmed by
            // comparing SQLRunner's CREATE INDEX output against our creation
            // path (see ParadoxHeaderBuilder.BuildSecondaryIndexHeader remarks).
            int indexOrdinal = 0;
            foreach (var index in schema.Indexes)
            {
                string ordinal = (indexOrdinal++).ToString();
                string xExt = ".XG" + ordinal;
                string yExt = ".YG" + ordinal;

                string xPath = Path.ChangeExtension(dbFilePath, xExt);
                byte[] xHeader = ParadoxHeaderBuilder.BuildSecondaryIndexHeader(schema, index);
                File.WriteAllBytes(xPath, xHeader);

                string yPath = Path.ChangeExtension(dbFilePath, yExt);
                byte[] yHeader = ParadoxHeaderBuilder.BuildMaintainedFieldHeader(schema, index);
                File.WriteAllBytes(yPath, yHeader);
            }
        }

        /// <summary>
        /// True for field types whose values are stored out-of-line in an
        /// accompanying .MB blob file rather than inline in the .DB record.
        /// </summary>
        private static bool IsBlobFieldType(ParadoxFieldTypes type)
        {
            switch (type)
            {
                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                    return true;
                default:
                    return false;
            }
        }
    }
}
