using System;
using System.IO;
using System.Windows.Forms;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// Read-only dialog (Table > Info Structure) showing a table's field
    /// schema (name/type/size/primary-key membership) and any discovered
    /// secondary index files. Editing structure is a possible future
    /// enhancement; for now this is purely informational.
    /// </summary>
    public partial class TableInfoForm : Form
    {
        public TableInfoForm(ParadoxTableFile table)
        {
            InitializeComponent();

            if (table == null) return;

            summaryLabel.Text = string.Format("{0}  —  {1} field(s), {2} record(s){3}",
                Path.GetFileName(table.FilePath),
                table.FieldCount,
                table.RecordCount,
                table.IndexOutOfDate ? "  [INDEX OUT OF DATE]" : string.Empty);

            int primaryKeyCount = table.primaryKeyFields;

            for (int i = 0; i < table.FieldNames.Length; i++)
            {
                var field = table.FieldTypes[i];
                bool isPrimaryKey = i < primaryKeyCount;

                var item = new ListViewItem(table.FieldNames[i]);
                item.SubItems.Add(field.fType.ToString());
                item.SubItems.Add(field.fSize.ToString());
                item.SubItems.Add(isPrimaryKey ? "Yes" : string.Empty);
                fieldsListView.Items.Add(item);
            }

            if (table.SecondaryIndexes.Count == 0)
            {
                indexesListBox.Items.Add("(none)");
            }
            else
            {
                foreach (var index in table.SecondaryIndexes)
                    indexesListBox.Items.Add(Path.GetFileName(index.FilePath));
            }
        }
    }
}
