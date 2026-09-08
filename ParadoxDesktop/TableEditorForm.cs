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
    /// table via a <see cref="DataGridView"/> running in virtual mode. Rows
    /// are decoded from the underlying <see cref="ParadoxTableFile"/> only
    /// on demand (as the grid actually needs them for display/scrolling),
    /// via <see cref="RowCursor"/>, instead of loading the entire table into
    /// memory up front. Edits are pushed back to the table via
    /// <see cref="ParadoxTableFile.UpdateRecord"/> as cells are committed.
    /// </summary>
    public partial class TableEditorForm : Form
    {
        private ParadoxTableFile table;
        private RowCursor rowCursor;

        private bool editModeEnabled;

        private readonly UndoRedoManager undoRedoManager = new UndoRedoManager();

        // Guards CellValuePushed so that values applied by Undo/Redo aren't
        // themselves recorded as new history entries.
        private bool suppressHistory;

        public string TableFilePath => table?.FilePath;

        public bool EditModeEnabled => editModeEnabled;

        public TableEditorForm(string dbFilePath)
        {
            InitializeComponent();
            dataGridView.AutoGenerateColumns = false;
            dataGridView.VirtualMode = true;
            dataGridView.CellValueNeeded += dataGridView_CellValueNeeded;
            dataGridView.CellValuePushed += dataGridView_CellValuePushed;
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

                var response = MessageBox.Show(this,
                    "One or more indexes for this table are out of date.\r\n\r\n" +
                    "Would you like to rebuild the table now?\r\n" +
                    "Otherwise, you'll only be able to read the rows.",
                    "Index Out Of Date", MessageBoxButtons.YesNo, MessageBoxIcon.Warning);

                if (response == DialogResult.Yes)
                {
                    RebuildTable();
                    return;
                }
            }

            SetupGrid();
        }

        /// <summary>
        /// (Re)builds the grid's column list and row cursor from the current
        /// state of <see cref="table"/>. Called on initial load and whenever
        /// the underlying block layout changes (rebuild, insert, delete).
        /// This is cheap even for very large tables: it only scans block
        /// headers/record counts, not individual field values, so no per-row
        /// decoding happens until the grid actually asks for a specific
        /// visible cell via <see cref="dataGridView_CellValueNeeded"/>.
        /// </summary>
        private void SetupGrid()
        {
            dataGridView.RowCount = 0;
            dataGridView.Columns.Clear();

            for (int i = 0; i < table.FieldNames.Length; i++)
            {
                var fieldType = table.FieldTypes[i];
                var column = new DataGridViewTextBoxColumn
                {
                    Name = table.FieldNames[i],
                    HeaderText = table.FieldNames[i],
                    ValueType = ColumnClrType(fieldType.fType),
                    ReadOnly = !editModeEnabled,
                };
                dataGridView.Columns.Add(column);
            }

            rowCursor = new RowCursor(table);
            dataGridView.RowCount = rowCursor.TotalRows;
            undoRedoManager.Clear();

            statusLabel.Text = string.Format("{0} record(s). {1}", table.RecordCount,
                editModeEnabled ? "Edit mode ON" : "Read-only (F9 to edit)");
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
            foreach (DataGridViewColumn column in dataGridView.Columns)
                column.ReadOnly = !editModeEnabled;
            statusLabel.Text = editModeEnabled
                ? string.Format("{0} record(s). Edit mode ON", table.RecordCount)
                : string.Format("{0} record(s). Read-only (F9 to edit)", table.RecordCount);
        }

        private void dataGridView_CurrentCellDirtyStateChanged(object sender, EventArgs e)
        {
            if (dataGridView.IsCurrentCellDirty)
                dataGridView.CommitEdit(DataGridViewDataErrorContexts.Commit);
        }

        /// <summary>
        /// Supplies the display value for a single grid cell on demand, decoding
        /// only the requested row's record from the underlying table (via
        /// <see cref="RowCursor"/>) rather than the whole table up front.
        /// </summary>
        private void dataGridView_CellValueNeeded(object sender, DataGridViewCellValueEventArgs e)
        {
            var record = rowCursor.GetRecord(e.RowIndex);
            if (record == null || e.ColumnIndex >= record.DataValues.Length)
            {
                e.Value = null;
                return;
            }

            e.Value = ToGridValue(record.DataValues[e.ColumnIndex]);
        }

        /// <summary>
        /// Applies a grid-edited cell value back to the underlying record and
        /// persists it via <see cref="ParadoxTableFile.UpdateRecord"/>.
        /// </summary>
        private void dataGridView_CellValuePushed(object sender, DataGridViewCellValueEventArgs e)
        {
            var record = rowCursor.GetRecord(e.RowIndex);
            if (record == null) return;

            var newValues = record.CloneDataValues();
            if (e.ColumnIndex >= newValues.Length) return;

            object oldValue = newValues[e.ColumnIndex];
            object newValue = FromGridValue(oldValue, e.Value);

            newValues[e.ColumnIndex] = newValue;

            try
            {
                table.UpdateRecord(record, newValues);
                statusLabel.Text = "Record updated.";

                if (!suppressHistory)
                    undoRedoManager.Push(new CellEditAction(this, e.RowIndex, e.ColumnIndex, oldValue, newValue));
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to save change:\r\n" + ex.Message, "Update",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        /// <summary>
        /// Applies a single cell value directly to the underlying record and
        /// persists it, without going through the grid's push/pull cycle.
        /// Used by <see cref="CellEditAction"/> to replay Undo/Redo.
        /// </summary>
        private void ApplyCellValue(int rowIndex, int columnIndex, object value)
        {
            var record = rowCursor.GetRecord(rowIndex);
            if (record == null) return;

            var newValues = record.CloneDataValues();
            if (columnIndex >= newValues.Length) return;

            newValues[columnIndex] = value;

            try
            {
                table.UpdateRecord(record, newValues);
                dataGridView.InvalidateCell(columnIndex, rowIndex);
                statusLabel.Text = "Record updated.";
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to save change:\r\n" + ex.Message, "Update",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        /// <summary>
        /// A single cell edit recorded on the undo/redo history stack. Undo
        /// restores the original value; Redo re-applies the edited value.
        /// </summary>
        private sealed class CellEditAction : IUndoableAction
        {
            private readonly TableEditorForm form;
            private readonly int rowIndex;
            private readonly int columnIndex;
            private readonly object oldValue;
            private readonly object newValue;

            public CellEditAction(TableEditorForm form, int rowIndex, int columnIndex, object oldValue, object newValue)
            {
                this.form = form;
                this.rowIndex = rowIndex;
                this.columnIndex = columnIndex;
                this.oldValue = oldValue;
                this.newValue = newValue;
            }

            public void Undo()
            {
                form.suppressHistory = true;
                try
                {
                    form.ApplyCellValue(rowIndex, columnIndex, oldValue);
                }
                finally
                {
                    form.suppressHistory = false;
                }
            }

            public void Redo()
            {
                form.suppressHistory = true;
                try
                {
                    form.ApplyCellValue(rowIndex, columnIndex, newValue);
                }
                finally
                {
                    form.suppressHistory = false;
                }
            }
        }

        // ----------------------------------------------------------------
        // Edit menu (Undo/Redo/Cut/Copy/Paste/Select All)
        // ----------------------------------------------------------------

        public void Undo()
        {
            if (!undoRedoManager.CanUndo) return;
            undoRedoManager.Undo();
        }

        public void Redo()
        {
            if (!undoRedoManager.CanRedo) return;
            undoRedoManager.Redo();
        }

        public void Cut()
        {
            if (!editModeEnabled) return;
            Copy();
            ClearSelectedCells();
        }

        public void Copy()
        {
            if (dataGridView.GetCellCount(DataGridViewElementStates.Selected) == 0) return;
            var dataObject = dataGridView.GetClipboardContent();
            if (dataObject != null)
                Clipboard.SetDataObject(dataObject);
        }

        public void Paste()
        {
            if (!editModeEnabled) return;
            if (!Clipboard.ContainsText()) return;

            string clipboardText = Clipboard.GetText();
            var lines = clipboardText.Replace("\r\n", "\n").TrimEnd('\n').Split('\n');

            var currentCell = dataGridView.CurrentCell;
            if (currentCell == null) return;

            int startRow = currentCell.RowIndex;
            int startCol = currentCell.ColumnIndex;

            for (int r = 0; r < lines.Length; r++)
            {
                int rowIndex = startRow + r;
                if (rowIndex >= dataGridView.RowCount) break;

                var cells = lines[r].Split('\t');
                for (int c = 0; c < cells.Length; c++)
                {
                    int colIndex = startCol + c;
                    if (colIndex >= dataGridView.ColumnCount) break;
                    if (dataGridView.Columns[colIndex].ReadOnly) continue;

                    dataGridView[colIndex, rowIndex].Value = cells[c];
                }
            }
        }

        public void SelectAllCells()
        {
            dataGridView.SelectAll();
        }

        private void ClearSelectedCells()
        {
            foreach (DataGridViewCell cell in dataGridView.SelectedCells)
            {
                if (!cell.OwningColumn.ReadOnly)
                    cell.Value = null;
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
            else if (e.KeyCode == Keys.Insert && e.Modifiers == Keys.None)
            {
                InsertRecord();
                e.Handled = true;
            }
            else if (e.KeyCode == Keys.Delete && e.Modifiers == Keys.Control)
            {
                DeleteCurrentRecord();
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

            if (!editModeEnabled)
            {
                MessageBox.Show(this, "Enable Edit Mode (F9) before modifying a memo/blob field.", "Modify Memo/Blob",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            int rowIndex = dataGridView.CurrentCell.RowIndex;
            int colIndex = dataGridView.CurrentCell.ColumnIndex;
            if (rowIndex < 0 || colIndex < 0 || rowIndex >= rowCursor.TotalRows) return;

            if (!IsMemoOrBlobColumn(colIndex))
            {
                MessageBox.Show(this, "The selected cell is not a memo or blob field.", "Modify Memo/Blob",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            var record = rowCursor.GetRecord(rowIndex);
            if (record == null) return;

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
                    dataGridView.InvalidateCell(colIndex, rowIndex);
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
        // Insert record (Ins)
        // ----------------------------------------------------------------

        /// <summary>
        /// Appends a new blank record to the table (via <see cref="ParadoxTableFile.AppendRecord"/>)
        /// and adds a corresponding blank row to the grid, ready for editing.
        /// </summary>
        public void InsertRecord()
        {
            if (table == null) return;

            if (!editModeEnabled)
            {
                MessageBox.Show(this, "Enable Edit Mode (F9) before inserting a record.", "Insert Record",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            try
            {
                var blankValues = new object[table.FieldTypes.Length];
                for (int i = 0; i < blankValues.Length; i++)
                {
                    if (table.FieldTypes[i].fType == ParadoxFieldTypes.MemoBLOb ||
                        table.FieldTypes[i].fType == ParadoxFieldTypes.FmtMemoBLOb)
                        blankValues[i] = new MemoValue(string.Empty, null);
                }

                table.AppendRecord(blankValues);

                // A new block may have been allocated (or the last block's
                // record count changed), so refresh the row cursor's block
                // map rather than trying to patch it incrementally.
                rowCursor.Refresh();
                dataGridView.RowCount = rowCursor.TotalRows;
                undoRedoManager.Clear();

                int newRowIndex = rowCursor.TotalRows - 1;
                dataGridView.ClearSelection();
                if (newRowIndex >= 0)
                    dataGridView.CurrentCell = dataGridView.Rows[newRowIndex].Cells[0];

                statusLabel.Text = string.Format("{0} record(s). Record inserted.", table.RecordCount);
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to insert record:\r\n" + ex.Message, "Insert Record",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        // ----------------------------------------------------------------
        // Delete record (Ctrl+Del)
        // ----------------------------------------------------------------

        /// <summary>
        /// Deletes the record backing the currently selected grid row (via
        /// <see cref="ParadoxTableFile.DeleteRecord"/>) after confirmation, and
        /// removes the corresponding row from the grid. Note: this only removes
        /// the .DB record; any memo/blob content it referenced in the .MB file
        /// is left orphaned (unreferenced) until the table is rebuilt via
        /// Table &gt; Table Rebuild, which reclaims the space.
        /// </summary>
        public void DeleteCurrentRecord()
        {
            if (table == null || dataGridView.CurrentCell == null) return;

            if (!editModeEnabled)
            {
                MessageBox.Show(this, "Enable Edit Mode (F9) before deleting a record.", "Delete Record",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            int rowIndex = dataGridView.CurrentCell.RowIndex;
            if (rowIndex < 0 || rowIndex >= rowCursor.TotalRows) return;

            var confirm = MessageBox.Show(this, "Delete the selected record? This cannot be undone.",
                "Delete Record", MessageBoxButtons.YesNo, MessageBoxIcon.Warning);
            if (confirm != DialogResult.Yes) return;

            try
            {
                var record = rowCursor.GetRecord(rowIndex);
                if (record == null) return;

                table.DeleteRecord(record);

                // Every row after the deleted one shifted down one slot within its
                // block (see ParadoxTableFile.DeleteRecord), so the cached
                // block/record positions for subsequent rows in the same block are
                // now stale. Refreshing the cursor is the simplest way to stay
                // consistent.
                rowCursor.Refresh();
                dataGridView.RowCount = rowCursor.TotalRows;
                undoRedoManager.Clear();

                statusLabel.Text = string.Format("{0} record(s). Record deleted.", table.RecordCount);
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to delete record:\r\n" + ex.Message, "Delete Record",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
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

                        foreach (var record in table.Enumerate())
                        {
                            var fields = new string[table.FieldNames.Length];
                            for (int i = 0; i < fields.Length && i < record.DataValues.Length; i++)
                            {
                                var gridValue = ToGridValue(record.DataValues[i]);
                                fields[i] = CsvEscape(gridValue == null || gridValue == DBNull.Value ? string.Empty : Convert.ToString(gridValue));
                            }
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

            using (var structureForm = new TableStructureForm(table))
                structureForm.ShowDialog(this);
        }

        public void ModifyStructure()
        {
            if (table == null) return;

            string dbFilePath = table.FilePath;

            bool useMemoryStreams = true; // TODO: either ask or make this a configurable setting in an options menu and saved in %programdata% or a config file etc. for now i'm testing it so it'll be true.

            using (var structureForm = new TableStructureForm(TableStructureMode.Modify, table))
            {
                if (structureForm.ShowDialog(this) != DialogResult.OK)
                    return;

                var newSchema = structureForm.Schema;

                try
                {
                    var result = TableRebuilder.RebuildWithSchema(table, newSchema, tempTableName: null, useMemoryStreams: useMemoryStreams);
                    table = new ParadoxTableFile(dbFilePath);
                    SetupGrid();

                    MessageBox.Show(this,
                        string.Format("Structure updated. {0} record(s) migrated across {1} file(s).",
                            result.RecordsMigrated, result.RebuiltFiles.Count),
                        "Modify Structure", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                catch (Exception ex)
                {
                    // TableRebuilder disposes the table even on failure paths that
                    // already got past opening it; reopen so this editor window stays usable.
                    table = new ParadoxTableFile(dbFilePath);
                    SetupGrid();

                    MessageBox.Show(this, "Modify structure failed:\r\n" + ex.Message, "Modify Structure",
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        // ----------------------------------------------------------------
        // Table Rebuild
        // ----------------------------------------------------------------

        public void RebuildTable()
        {
            if (table == null) return;

            string dbFilePath = table.FilePath;

            bool useMemoryStreams = true; // TODO: either ask or make this a configurable setting in an options menu and saved in %programdata% or a config file etc. for now i'm testing it so it'll be true.

            try
            {
                var result = TableRebuilder.Rebuild(table, tempTableName: null, useMemoryStreams: useMemoryStreams);
                table = new ParadoxTableFile(dbFilePath);
                SetupGrid();

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
                SetupGrid();

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
