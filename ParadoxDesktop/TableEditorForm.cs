using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// Floating MDI child window that displays and edits a single Paradox
    /// table via a <see cref="DataGridView"/>. All rows are loaded into an
    /// in-memory <see cref="DataTable"/> for simple two-way grid binding;
    /// edits are pushed back to the underlying <see cref="ParadoxTableFile"/>
    /// via <see cref="UpdateRecord"/> as cells are committed.
    /// </summary>
    public partial class TableEditorForm : Form
    {
        private ParadoxTableFile table;
        private DataTable dataTable;
        private BindingSource bindingSource;

        // Parallel to the DataTable rows: the ParadoxRecord each row came from,
        // so edits can be written back to the correct block/record position.
        private List<ParadoxRecord> rowRecords;

        private bool editModeEnabled;
        private bool suppressCellChangeHandling;

        public string TableFilePath => table?.FilePath;

        public bool EditModeEnabled => editModeEnabled;

        public TableEditorForm(string dbFilePath)
        {
            InitializeComponent();
            bindingSource = new BindingSource();
            dataGridView.AutoGenerateColumns = true;
            dataGridView.CellValueChanged += dataGridView_CellValueChanged;
            dataGridView.CurrentCellDirtyStateChanged += dataGridView_CurrentCellDirtyStateChanged;
            LoadTable(dbFilePath);
        }

        // ----------------------------------------------------------------
        // Loading
        // ----------------------------------------------------------------

        private void LoadTable(string dbFilePath)
        {
            table = new ParadoxTableFile(dbFilePath);
            Text = Path.GetFileNameWithoutExtension(dbFilePath);

            if (table.IndexOutOfDate)
            {
                statusLabel.Text = "Warning: one or more indexes are out of date. Use Table > Table Rebuild to fix.";
            }

            RebuildGridFromTable();
        }

        /// <summary>
        /// (Re)builds the in-memory DataTable/grid contents from the current
        /// state of <see cref="table"/>. Called on initial load and after a
        /// rebuild replaces the underlying files.
        /// </summary>
        private void RebuildGridFromTable()
        {
            suppressCellChangeHandling = true;
            try
            {
                dataTable = new DataTable();
                for (int i = 0; i < table.FieldNames.Length; i++)
                {
                    var fieldType = table.FieldTypes[i];
                    dataTable.Columns.Add(table.FieldNames[i], ColumnClrType(fieldType.fType));
                }

                rowRecords = new List<ParadoxRecord>();
                foreach (var record in table.Enumerate())
                {
                    var row = dataTable.NewRow();
                    for (int i = 0; i < record.DataValues.Length && i < dataTable.Columns.Count; i++)
                        row[i] = ToGridValue(record.DataValues[i]);
                    dataTable.Rows.Add(row);
                    rowRecords.Add(record);
                }

                bindingSource.DataSource = dataTable;
                dataGridView.DataSource = bindingSource;

                statusLabel.Text = string.Format("{0} record(s). {1}", table.RecordCount,
                    editModeEnabled ? "Edit mode ON" : "Read-only (F9 to edit)");
            }
            finally
            {
                suppressCellChangeHandling = false;
            }
        }

        /// <summary>Maps a Paradox field type to the CLR type used for its grid column.</summary>
        private static Type ColumnClrType(ParadoxFieldTypes fType)
        {
            switch (fType)
            {
                case ParadoxFieldTypes.Short:
                    return typeof(short);
                case ParadoxFieldTypes.Long:
                case ParadoxFieldTypes.AutoInc:
                    return typeof(int);
                case ParadoxFieldTypes.Currency:
                case ParadoxFieldTypes.Number:
                    return typeof(double);
                case ParadoxFieldTypes.Date:
                case ParadoxFieldTypes.Timestamp:
                    return typeof(DateTime);
                case ParadoxFieldTypes.Time:
                    return typeof(TimeSpan);
                case ParadoxFieldTypes.Logical:
                    return typeof(bool);
                case ParadoxFieldTypes.BCD:
                    return typeof(decimal);
                default:
                    // Alpha, Memo/FmtMemo (rendered as text), BLOb/OLE/Graphic/Bytes (rendered as a placeholder string)
                    return typeof(string);
            }
        }

        /// <summary>Converts a raw ParadoxRecord field value into something displayable/editable in the grid.</summary>
        private static object ToGridValue(object value)
        {
            if (value == null) return DBNull.Value;

            var memo = value as MemoValue;
            if (memo != null) return memo.Text ?? string.Empty;

            var bytes = value as byte[];
            if (bytes != null) return string.Format("<binary: {0} byte(s)>", bytes.Length);

            return value;
        }

        // ----------------------------------------------------------------
        // Edit mode (F9)
        // ----------------------------------------------------------------

        public void ToggleEditMode()
        {
            editModeEnabled = !editModeEnabled;
            dataGridView.ReadOnly = !editModeEnabled;
            statusLabel.Text = editModeEnabled
                ? string.Format("{0} record(s). Edit mode ON", table.RecordCount)
                : string.Format("{0} record(s). Read-only (F9 to edit)", table.RecordCount);
        }

        private void dataGridView_CurrentCellDirtyStateChanged(object sender, EventArgs e)
        {
            if (dataGridView.IsCurrentCellDirty)
                dataGridView.CommitEdit(DataGridViewDataErrorContexts.Commit);
        }

        private void dataGridView_CellValueChanged(object sender, DataGridViewCellEventArgs e)
        {
            if (suppressCellChangeHandling) return;
            if (e.RowIndex < 0 || e.RowIndex >= rowRecords.Count) return;

            CommitRowToTable(e.RowIndex);
        }

        private void CommitRowToTable(int rowIndex)
        {
            var record = rowRecords[rowIndex];
            var newValues = record.CloneDataValues();

            var dataRow = dataTable.Rows[rowIndex];
            for (int i = 0; i < newValues.Length && i < dataTable.Columns.Count; i++)
            {
                var gridValue = dataRow[i];
                newValues[i] = FromGridValue(newValues[i], gridValue);
            }

            try
            {
                table.UpdateRecord(record, newValues);
                statusLabel.Text = "Record updated.";
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to save change:\r\n" + ex.Message, "Update",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        /// <summary>Converts a grid-edited value back into the field's original representation (preserving MemoValue.BlobInfo).</summary>
        private static object FromGridValue(object originalValue, object gridValue)
        {
            var originalMemo = originalValue as MemoValue;
            if (originalMemo != null)
            {
                string text = gridValue == null || gridValue == DBNull.Value ? string.Empty : gridValue.ToString();
                return new MemoValue(text, originalMemo.BlobInfo);
            }

            // Binary (byte[]) fields aren't editable inline in the grid (shown as a placeholder);
            // keep the original reference bytes untouched unless changed via the memo/blob editor.
            if (originalValue is byte[])
                return originalValue;

            if (gridValue == DBNull.Value) return null;
            return gridValue;
        }

        // ----------------------------------------------------------------
        // Modify Memo/Blob (F2)
        // ----------------------------------------------------------------

        private void dataGridView_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.F2)
            {
                ModifyCurrentMemoOrBlob();
                e.Handled = true;
            }
        }

        private void dataGridView_CellDoubleClick(object sender, DataGridViewCellEventArgs e)
        {
            if (e.RowIndex < 0 || e.ColumnIndex < 0) return;
            if (IsMemoOrBlobColumn(e.ColumnIndex))
                ModifyCurrentMemoOrBlob();
        }

        private bool IsMemoOrBlobColumn(int columnIndex)
        {
            if (table == null || columnIndex < 0 || columnIndex >= table.FieldTypes.Length) return false;
            switch (table.FieldTypes[columnIndex].fType)
            {
                case ParadoxFieldTypes.MemoBLOb:
                case ParadoxFieldTypes.FmtMemoBLOb:
                case ParadoxFieldTypes.BLOb:
                case ParadoxFieldTypes.OLE:
                case ParadoxFieldTypes.Graphic:
                case ParadoxFieldTypes.Bytes:
                    return true;
                default:
                    return false;
            }
        }

        public void ModifyCurrentMemoOrBlob()
        {
            if (table == null || dataGridView.CurrentCell == null) return;

            int rowIndex = dataGridView.CurrentCell.RowIndex;
            int colIndex = dataGridView.CurrentCell.ColumnIndex;
            if (rowIndex < 0 || colIndex < 0 || rowIndex >= rowRecords.Count) return;

            if (!IsMemoOrBlobColumn(colIndex))
            {
                MessageBox.Show(this, "The selected cell is not a memo or blob field.", "Modify Memo/Blob",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            var record = rowRecords[rowIndex];
            var fieldType = table.FieldTypes[colIndex].fType;
            var currentValue = record.DataValues[colIndex];

            using (var editor = new MemoEditorForm(table.FieldNames[colIndex], fieldType, currentValue, editModeEnabled))
            {
                if (editor.ShowDialog(this) != DialogResult.OK) return;

                var newValues = record.CloneDataValues();
                newValues[colIndex] = editor.ResultValue;

                try
                {
                    table.UpdateRecord(record, newValues);
                    dataTable.Rows[rowIndex][colIndex] = ToGridValue(newValues[colIndex]);
                    statusLabel.Text = "Memo/blob field updated.";
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, "Failed to save change:\r\n" + ex.Message, "Modify Memo/Blob",
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        // ----------------------------------------------------------------
        // Save As
        // ----------------------------------------------------------------

        public void SaveAs()
        {
            if (table == null) return;

            using (var dlg = new SaveFileDialog())
            {
                dlg.Filter = "Paradox tables (*.db)|*.db|All files (*.*)|*.*";
                dlg.DefaultExt = "db";
                dlg.FileName = Path.GetFileName(table.FilePath);
                dlg.InitialDirectory = Path.GetDirectoryName(table.FilePath);

                if (dlg.ShowDialog(this) != DialogResult.OK) return;

                try
                {
                    string destDir = Path.GetDirectoryName(dlg.FileName);
                    string destBaseName = Path.GetFileNameWithoutExtension(dlg.FileName);
                    string sourceDir = Path.GetDirectoryName(table.FilePath);
                    string sourceBaseName = Path.GetFileNameWithoutExtension(table.FilePath);

                    // Copy every associated file (.DB/.PX/.Xnn/.Xgn/.Ynn/.Ygn/.MB) under the new base name.
                    foreach (var sourceFile in Directory.GetFiles(sourceDir, sourceBaseName + ".*"))
                    {
                        string ext = Path.GetExtension(sourceFile);
                        string destFile = Path.Combine(destDir, destBaseName + ext);
                        File.Copy(sourceFile, destFile, overwrite: true);
                    }

                    MessageBox.Show(this, "Table saved as:\r\n" + dlg.FileName, "Save As",
                        MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, "Save As failed:\r\n" + ex.Message, "Save As",
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        // ----------------------------------------------------------------
        // Export CSV
        // ----------------------------------------------------------------

        public void ExportCsv()
        {
            if (table == null) return;

            using (var dlg = new SaveFileDialog())
            {
                dlg.Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*";
                dlg.DefaultExt = "csv";
                dlg.FileName = Path.GetFileNameWithoutExtension(table.FilePath) + ".csv";

                if (dlg.ShowDialog(this) != DialogResult.OK) return;

                try
                {
                    using (var writer = new StreamWriter(dlg.FileName, false, Encoding.UTF8))
                    {
                        writer.WriteLine(string.Join(",", table.FieldNames.Select(CsvEscape).ToArray()));

                        foreach (DataRow row in dataTable.Rows)
                        {
                            var fields = new string[dataTable.Columns.Count];
                            for (int i = 0; i < dataTable.Columns.Count; i++)
                                fields[i] = CsvEscape(row[i] == DBNull.Value ? string.Empty : Convert.ToString(row[i]));
                            writer.WriteLine(string.Join(",", fields));
                        }
                    }

                    MessageBox.Show(this, "Exported to:\r\n" + dlg.FileName, "Export",
                        MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, "Export failed:\r\n" + ex.Message, "Export",
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        private static string CsvEscape(string value)
        {
            if (value == null) return string.Empty;
            if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            return value;
        }

        // ----------------------------------------------------------------
        // Info Structure
        // ----------------------------------------------------------------

        public void ShowInfoStructure()
        {
            if (table == null) return;

            using (var infoForm = new TableInfoForm(table))
                infoForm.ShowDialog(this);
        }

        // ----------------------------------------------------------------
        // Table Rebuild
        // ----------------------------------------------------------------

        public void RebuildTable()
        {
            if (table == null) return;

            string dbFilePath = table.FilePath;

            try
            {
                var result = TableRebuilder.Rebuild(table);
                table = new ParadoxTableFile(dbFilePath);
                RebuildGridFromTable();

                MessageBox.Show(this,
                    string.Format("Rebuild complete. {0} record(s) migrated across {1} file(s).",
                        result.RecordsMigrated, result.RebuiltFiles.Count),
                    "Table Rebuild", MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
            catch (Exception ex)
            {
                // TableRebuilder.Rebuild disposes the table even on failure paths that
                // already got past opening it; reopen so this editor window stays usable.
                table = new ParadoxTableFile(dbFilePath);
                RebuildGridFromTable();

                MessageBox.Show(this, "Rebuild failed:\r\n" + ex.Message, "Table Rebuild",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        // ----------------------------------------------------------------
        // Cleanup
        // ----------------------------------------------------------------

        private void TableEditorForm_FormClosed(object sender, FormClosedEventArgs e)
        {
            CloseTable();
        }

        private void CloseTable()
        {
            if (table != null)
            {
                table.Dispose();
                table = null;
            }
        }
    }
}
