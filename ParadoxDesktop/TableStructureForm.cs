using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Windows.Forms;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// Mode a <see cref="TableStructureForm"/> is opened in.
    /// </summary>
    public enum TableStructureMode
    {
        /// <summary>Read-only "Table > Info Structure" view of an existing table.</summary>
        ViewOnly,

        /// <summary>"File > New > Table": build a brand-new table's structure from scratch.</summary>
        Create,

        /// <summary>"Table > Modify Structure": edit an existing table's structure.</summary>
        Modify
    }

    /// <summary>
    /// Dual-purpose table structure dialog: shows a table's field/index
    /// schema read-only (<see cref="TableStructureMode.ViewOnly"/>), or lets
    /// the user define/edit fields and indexes for creating a new table
    /// (<see cref="TableStructureMode.Create"/>) or modifying an existing
    /// one's structure (<see cref="TableStructureMode.Modify"/>).
    /// </summary>
    public partial class TableStructureForm : Form
    {
        private readonly TableStructureMode mode;
        private readonly ParadoxTableFile existingTable;

        /// <summary>
        /// The edited schema. Populated from the source table (Modify) or
        /// starts empty (Create); reflects whatever the user has entered
        /// once the dialog returns <see cref="DialogResult.OK"/>.
        /// </summary>
        public TableSchemaDefinition Schema { get; private set; }

        /// <summary>Opens in <see cref="TableStructureMode.ViewOnly"/> for an existing, already-open table.</summary>
        public TableStructureForm(ParadoxTableFile table) : this(TableStructureMode.ViewOnly, table)
        {
        }

        /// <summary>
        /// Opens in <paramref name="mode"/>. For <see cref="TableStructureMode.ViewOnly"/>
        /// or <see cref="TableStructureMode.Modify"/>, <paramref name="table"/> must be
        /// the table being viewed/modified. For <see cref="TableStructureMode.Create"/>,
        /// <paramref name="table"/> must be null.
        /// </summary>
        public TableStructureForm(TableStructureMode mode, ParadoxTableFile table)
        {
            this.mode = mode;
            this.existingTable = table;

            InitializeComponent();

            switch (mode)
            {
                case TableStructureMode.ViewOnly:
                    Text = "Table Structure";
                    Schema = TableSchemaDefinition.FromTable(table);
                    ConfigureViewOnly();
                    break;

                case TableStructureMode.Create:
                    Text = "New Table";
                    Schema = new TableSchemaDefinition();
                    ConfigureEditable();
                    break;

                case TableStructureMode.Modify:
                    Text = "Modify Table Structure";
                    Schema = TableSchemaDefinition.FromTable(table);
                    ConfigureEditable();
                    break;
            }

            RefreshFieldsList();
            RefreshIndexesList();
        }

        // ------------------------------------------------------------
        // ViewOnly mode
        // ------------------------------------------------------------

        private void ConfigureViewOnly()
        {
            editorPanel.Visible = false;
            indexButtonsPanel.Visible = false;
            tableNameLabel.Visible = false;
            tableNameTextBox.Visible = false;
            okButton.Visible = false;
            cancelButton.Text = "Close";
            cancelButton.DialogResult = DialogResult.Cancel;

            if (existingTable != null)
            {
                summaryLabel.Text = string.Format("{0}  —  {1} field(s), {2} record(s){3}",
                    Path.GetFileName(existingTable.FilePath),
                    existingTable.FieldCount,
                    existingTable.RecordCount,
                    existingTable.IndexOutOfDate ? "  [INDEX OUT OF DATE]" : string.Empty);
            }
        }

        // ------------------------------------------------------------
        // Create/Modify mode
        // ------------------------------------------------------------

        private void ConfigureEditable()
        {
            summaryLabel.Text = mode == TableStructureMode.Create
                ? "Define the new table's fields and indexes below."
                : "Edit the table's fields and indexes below, then click OK to rebuild the table.";

            tableNameTextBox.Text = Schema.TableName ?? string.Empty;
            tableNameTextBox.Enabled = mode == TableStructureMode.Create;

            fieldTypeComboBox.DataSource = Enum.GetValues(typeof(ParadoxFieldTypes));

            fieldsListView.MultiSelect = false;
            fieldsListView.SelectedIndexChanged += fieldsListView_SelectedIndexChanged;
            fieldTypeComboBox.SelectedIndexChanged += fieldTypeComboBox_SelectedIndexChanged;

            // Long is overwhelmingly the most common first field (e.g. an ID/primary
            // key), so default to it for the very first field only. Set this after
            // wiring SelectedIndexChanged above so the fixed-size/disable logic in
            // fieldTypeComboBox_SelectedIndexChanged actually runs for this default,
            // matching what happens when a user manually picks Long from the dropdown.
            fieldTypeComboBox.SelectedItem = ParadoxFieldTypes.Long;
        }

        private void fieldTypeComboBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            if (!(fieldTypeComboBox.SelectedItem is ParadoxFieldTypes type)) return;

            byte? fixedSize = ParadoxFieldTypeSizes.GetFixedSize(type);
            if (fixedSize.HasValue)
            {
                fieldSizeNumericUpDown.Value = Math.Max(fieldSizeNumericUpDown.Minimum,
                    Math.Min(fieldSizeNumericUpDown.Maximum, fixedSize.Value));
            }
            fieldSizeNumericUpDown.Enabled = !fixedSize.HasValue;
        }

        private void RefreshFieldsList()
        {
            fieldsListView.Items.Clear();
            for (int i = 0; i < Schema.Fields.Count; i++)
            {
                var f = Schema.Fields[i];
                var item = new ListViewItem(f.Name);
                item.SubItems.Add(f.Type.ToString());
                // Blob-capable fields (including memo, a blob sub-type) store the
                // on-disk fSize, which includes a 10-byte pointer (offset+index,
                // size, mod_nr) in addition to the inline "leader" bytes. Users
                // think of the size as the leader/BLOB(n,...) value (as SQL/BDE
                // express it), so subtract the pointer overhead back out for
                // display purposes only.
                int displaySize = TableFieldDefinition.IsBlobType(f.Type) ? Math.Max(0, f.Size - 10) : f.Size;
                item.SubItems.Add(displaySize.ToString());
                item.SubItems.Add(f.IsPrimaryKey ? "Yes" : string.Empty);
                item.Tag = f;
                fieldsListView.Items.Add(item);
            }
        }

        private void RefreshIndexesList()
        {
            indexesListBox.Items.Clear();

            if (Schema.Indexes.Count == 0)
            {
                indexesListBox.Items.Add("(none)");
                return;
            }

            foreach (var index in Schema.Indexes)
            {
                var fieldNames = index.FieldIndices
                    .Where(i => i >= 0 && i < Schema.Fields.Count)
                    .Select(i => Schema.Fields[i].Name)
                    .ToArray();
                indexesListBox.Items.Add(string.Join(", ", fieldNames));
            }
        }

        private TableFieldDefinition SelectedField =>
            fieldsListView.SelectedItems.Count > 0 ? (TableFieldDefinition)fieldsListView.SelectedItems[0].Tag : null;

        private void fieldsListView_SelectedIndexChanged(object sender, EventArgs e)
        {
            var field = SelectedField;
            if (field == null)
            {
                ResetFieldEditor();
                return;
            }

            addUpdateFieldButton.Text = "Update Field";
            fieldNameTextBox.Text = field.Name;
            fieldTypeComboBox.SelectedItem = field.Type;
            // Blob-capable fields (including memo) are edited in leader-size terms,
            // matching the on-disk fSize minus the 10-byte pointer overhead.
            int editSize = TableFieldDefinition.IsBlobType(field.Type) ? Math.Max(0, field.Size - 10) : field.Size;
            fieldSizeNumericUpDown.Value = Math.Max(fieldSizeNumericUpDown.Minimum, Math.Min(fieldSizeNumericUpDown.Maximum, editSize));
            fieldSizeNumericUpDown.Enabled = !ParadoxFieldTypeSizes.GetFixedSize(field.Type).HasValue;
            fieldPrimaryKeyCheckBox.Checked = field.IsPrimaryKey;
            removeFieldButton.Enabled = true;
        }

        private void addUpdateFieldButton_Click(object sender, EventArgs e)
        {
            string name = fieldNameTextBox.Text.Trim();
            if (name.Length == 0)
            {
                MessageBox.Show(this, "Enter a field name.", "Field", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            var type = (ParadoxFieldTypes)fieldTypeComboBox.SelectedItem;
            byte? fixedSize = ParadoxFieldTypeSizes.GetFixedSize(type);
            int enteredSize = fixedSize ?? (int)fieldSizeNumericUpDown.Value;
            bool isPrimaryKey = fieldPrimaryKeyCheckBox.Checked;
            bool isBlob = TableFieldDefinition.IsBlobType(type);

            var existing = SelectedField;
            bool duplicateName = Schema.Fields.Any(f =>
                !ReferenceEquals(f, existing) && string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase));
            if (duplicateName)
            {
                MessageBox.Show(this, "A field with that name already exists.", "Field", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            // Blob-capable fields (including memo) are entered in leader-size terms
            // (matching SQL's BLOB(n, ...) syntax); TableFieldDefinition.CreateBlobField
            // converts that to the on-disk fSize (leaderSize + 10 pointer bytes).
            if (isBlob)
            {
                TableFieldDefinition blobField;
                try
                {
                    blobField = TableFieldDefinition.CreateBlobField(name, type, enteredSize, isPrimaryKey);
                }
                catch (ArgumentOutOfRangeException ex)
                {
                    MessageBox.Show(this, ex.Message, "Field", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return;
                }

                if (existing != null)
                {
                    existing.Name = blobField.Name;
                    existing.Type = blobField.Type;
                    existing.Size = blobField.Size;
                    existing.IsPrimaryKey = blobField.IsPrimaryKey;
                }
                else
                {
                    Schema.Fields.Add(blobField);
                }
            }
            else
            {
                byte size = (byte)enteredSize;
                if (existing != null)
                {
                    existing.Name = name;
                    existing.Type = type;
                    existing.Size = size;
                    existing.IsPrimaryKey = isPrimaryKey;
                }
                else
                {
                    Schema.Fields.Add(new TableFieldDefinition(name, type, size, isPrimaryKey));
                }
            }

            Schema.ReorderPrimaryKeyFieldsFirst();
            RefreshFieldsList();
            RefreshIndexesList(); // field order may have shifted index field references' display
            fieldsListView.SelectedItems.Clear();

            // Re-adding the exact same field is highly unlikely to be intended, so
            // reset the editor controls to a fresh "Add Field" state rather than
            // leaving the just-added field's values in place.
            ResetFieldEditor();
        }

        /// <summary>
        /// Resets the field name/PK/remove-button editor controls to their default,
        /// "Add Field" state after adding/updating a field. Deliberately leaves the
        /// type/size combo alone (unlike the very first field, where Long is defaulted
        /// in ConfigureEditable) since re-adding the same *name* is unlikely but the
        /// same *type* for a subsequent field is common (e.g. adding several Alpha
        /// fields in a row), and changing it back would also fight the type combo's
        /// own SelectedIndexChanged-driven size/enabled logic.
        /// </summary>
        private void ResetFieldEditor()
        {
            addUpdateFieldButton.Text = "Add Field";
            fieldNameTextBox.Text = string.Empty;
            fieldPrimaryKeyCheckBox.Checked = false;
            removeFieldButton.Enabled = false;
            fieldNameTextBox.Focus();
        }

        private void removeFieldButton_Click(object sender, EventArgs e)
        {
            var field = SelectedField;
            if (field == null) return;

            int removedIndex = Schema.Fields.IndexOf(field);
            if (Schema.Indexes.Any(idx => idx.FieldIndices.Contains(removedIndex)))
            {
                MessageBox.Show(this, "This field is used by one or more indexes. Remove those indexes first.",
                    "Field", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            Schema.Fields.Remove(field);

            // Shift down any index field references pointing past the removed field.
            foreach (var idx in Schema.Indexes)
            {
                for (int i = 0; i < idx.FieldIndices.Count; i++)
                    if (idx.FieldIndices[i] > removedIndex)
                        idx.FieldIndices[i]--;
            }

            RefreshFieldsList();
            RefreshIndexesList();
        }

        private void addEditIndexButton_Click(object sender, EventArgs e)
        {
            if (Schema.Fields.Count == 0)
            {
                MessageBox.Show(this, "Add at least one field before creating an index.", "Index", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            int selected = indexesListBox.SelectedIndex;
            bool isEdit = selected >= 0 && selected < Schema.Indexes.Count;
            var existing = isEdit ? Schema.Indexes[selected] : null;

            using (var editor = new IndexFieldsForm(Schema.Fields.Select(f => f.Name).ToList(), existing?.FieldIndices))
            {
                if (editor.ShowDialog(this) != DialogResult.OK)
                    return;

                if (editor.SelectedFieldIndices.Count == 0)
                    return;

                if (isEdit)
                    existing.FieldIndices = editor.SelectedFieldIndices;
                else
                    Schema.Indexes.Add(new TableIndexDefinition(editor.SelectedFieldIndices));

                RefreshIndexesList();
            }
        }

        private void removeIndexButton_Click(object sender, EventArgs e)
        {
            int selected = indexesListBox.SelectedIndex;
            if (selected < 0 || selected >= Schema.Indexes.Count) return;

            Schema.Indexes.RemoveAt(selected);
            RefreshIndexesList();
        }

        private void okButton_Click(object sender, EventArgs e)
        {
            if (Schema.Fields.Count == 0)
            {
                MessageBox.Show(this, "The table must have at least one field.", "Table Structure", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                DialogResult = DialogResult.None;
                return;
            }

            string name = tableNameTextBox.Text.Trim();
            if (mode == TableStructureMode.Create && name.Length == 0)
            {
                MessageBox.Show(this, "Enter a table name.", "Table Structure", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                DialogResult = DialogResult.None;
                return;
            }

            Schema.TableName = name;
        }
    }
}
