using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// MDI parent hosting the application's menu strip (File/Edit/Table/Help).
    /// Each opened Paradox table is shown as a separate floating MDI child
    /// window (<see cref="TableEditorForm"/>); menu commands that operate on
    /// "the current table" act on <see cref="Form.ActiveMdiChild"/>.
    /// </summary>
    public partial class ParadoxDesktopMainForm : Form
    {
        public ParadoxDesktopMainForm()
        {
            InitializeComponent();

            UpdateToolbarVisibility(false, false, false, false);
        }

        private TableEditorForm ActiveTableEditor => ActiveMdiChild as TableEditorForm;

        private SqlEditorForm ActiveSqlEditor => ActiveMdiChild as SqlEditorForm;

        // ----------------------------------------------------------------
        // MDI child activation: keep Table/Record/SQL menus in sync with
        // whichever kind of child window is currently active.
        // ----------------------------------------------------------------

        private void ParadoxDesktopMainForm_MdiChildActivate(object sender, EventArgs e)
        {
            bool anyActive = ActiveMdiChild != null;
            bool tableActive = ActiveTableEditor != null;
            bool sqlActive = ActiveSqlEditor != null;

            tableMenuItem.Enabled = tableActive;
            recordMenuItem.Enabled = tableActive;
            editModeToolButton.Checked = tableActive && ActiveTableEditor.EditModeEnabled;
            sqlMenuItem.Enabled = sqlActive;

            UpdateToolbarVisibility(anyActive, tableActive, tableActive, sqlActive);
        }

        // mainToolStrip holds four contextual groups of buttons, in order:
        // Edit (always shown when any MDI child is active), Record (table
        // editors only), Table (table editors only), and SQL (SQL editor
        // only). "|" labels visually separate every button; a separator is
        // only shown when both its neighboring groups are visible, so we
        // never end up with a dangling "|" at the start/end of the row.
        private void UpdateToolbarVisibility(bool editVisible, bool recordVisible, bool tableVisible, bool sqlVisible)
        {
            undoToolButton.Visible = editVisible;
            pipe1.Visible = editVisible;
            redoToolButton.Visible = editVisible;
            pipe2.Visible = editVisible;
            cutToolButton.Visible = editVisible;
            pipe3.Visible = editVisible;
            copyToolButton.Visible = editVisible;
            pipe4.Visible = editVisible;
            pasteToolButton.Visible = editVisible;
            pipe5.Visible = editVisible;
            selectAllToolButton.Visible = editVisible;

            insertRecordToolButton.Visible = recordVisible;
            pipe9.Visible = recordVisible;
            deleteRecordToolButton.Visible = recordVisible;
            pipe7.Visible = recordVisible;
            editModeToolButton.Visible = recordVisible;
            pipe8.Visible = recordVisible;
            modifyMemoToolButton.Visible = recordVisible;

            runSqlToolButton.Visible = sqlVisible;

            pipe6.Visible = editVisible && (recordVisible || sqlVisible);
            pipe10.Visible = recordVisible && sqlVisible;
        }

        // ----------------------------------------------------------------
        // File menu
        // ----------------------------------------------------------------

        private void newSqlFileMenuItem_Click(object sender, EventArgs e)
        {
            var editor = new SqlEditorForm
            {
                MdiParent = this
            };
            editor.Show();
        }


        private void newTableMenuItem_Click(object sender, EventArgs e)
        {


            using (var structureForm = new TableStructureForm(TableStructureMode.Create, null))
            {
                if (structureForm.ShowDialog(this) != DialogResult.OK)
                    return;

                string dbFilePath;
                using (var saveDialog = new SaveFileDialog
                {
                    Filter = "Paradox Tables (*.db)|*.db|All files (*.*)|*.*",
                    DefaultExt = "db",
                    FileName = structureForm.Schema.TableName
                })
                {
                    if (saveDialog.ShowDialog(this) != DialogResult.OK)
                        return;

                    dbFilePath = saveDialog.FileName;
                }

                try
                {
                    TableCreator.CreateNew(dbFilePath, structureForm.Schema);
                    OpenTable(dbFilePath);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, "Failed to create table:\r\n" + ex.Message, "New Table",
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        private void openMenuItem_Click(object sender, EventArgs e)
        {
            if (openFileDialog.ShowDialog(this) != DialogResult.OK)
                return;

            OpenPath(openFileDialog.FileName);
        }

        /// <summary>
        /// Opens <paramref name="path"/> in the appropriate MDI child window
        /// based on its extension: .sql opens a <see cref="SqlEditorForm"/>
        /// with the script loaded, anything else (e.g. .db) opens a
        /// <see cref="TableEditorForm"/>.
        /// </summary>
        internal void OpenPath(string path)
        {
            if (string.Equals(Path.GetExtension(path), ".sql", StringComparison.OrdinalIgnoreCase))
                OpenSqlFile(path);
            else
                OpenTable(path);
        }

        internal void OpenSqlFile(string sqlFilePath)
        {
            try
            {
                var editor = new SqlEditorForm
                {
                    MdiParent = this
                };
                editor.LoadFromFile(sqlFilePath);
                editor.Show();
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to open SQL file:\r\n" + ex.Message, "Open",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        internal void OpenTable(string dbFilePath)
        {
            try
            {
                var editor = new TableEditorForm(dbFilePath)
                {
                    MdiParent = this
                };
                editor.Show();
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to open table:\r\n" + ex.Message, "Open",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void saveAsMenuItem_Click(object sender, EventArgs e)
        {
            if (ActiveTableEditor != null)
            {
                ActiveTableEditor.SaveAs();
                return;
            }

            if (ActiveSqlEditor != null)
            {
                ActiveSqlEditor.SaveAs();
                return;
            }

            MessageBox.Show(this, "No table or SQL file is currently open.", "Save As",
                MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        private void exportMenuItem_Click(object sender, EventArgs e)
        {
            if (ActiveTableEditor != null)
            {
                ActiveTableEditor.ExportCsv();
                return;
            }

            if (ActiveSqlEditor != null)
            {
                ActiveSqlEditor.ExportCsv();
                return;
            }

            MessageBox.Show(this, "No table or SQL results are currently open.", "Export",
                MessageBoxButtons.OK, MessageBoxIcon.Information);
        }

        private void exitMenuItem_Click(object sender, EventArgs e)
        {
            Close();
        }

        // ----------------------------------------------------------------
        // Edit menu
        // ----------------------------------------------------------------

        private void editModeMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                editModeToolButton.Checked = false;
                MessageBox.Show(this, "No table is currently open.", "Edit Mode",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.ToggleEditMode();
            editModeToolButton.Checked = editor.EditModeEnabled;
        }

        private void modifyMemoMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No table is currently open.", "Modify Memo/Blob",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.ModifyCurrentMemoOrBlob();
        }

        private void undoMenuItem_Click(object sender, EventArgs e)
        {
            ActiveTableEditor?.Undo();
        }

        private void redoMenuItem_Click(object sender, EventArgs e)
        {
            ActiveTableEditor?.Redo();
        }

        private void cutMenuItem_Click(object sender, EventArgs e)
        {
            if (ActiveTableEditor != null)
                ActiveTableEditor.Cut();
            else
                ActiveSqlEditor?.Cut();
        }

        private void copyMenuItem_Click(object sender, EventArgs e)
        {
            if (ActiveTableEditor != null)
                ActiveTableEditor.Copy();
            else
                ActiveSqlEditor?.Copy();
        }

        private void pasteMenuItem_Click(object sender, EventArgs e)
        {
            if (ActiveTableEditor != null)
                ActiveTableEditor.Paste();
            else
                ActiveSqlEditor?.Paste();
        }

        private void selectAllMenuItem_Click(object sender, EventArgs e)
        {
            if (ActiveTableEditor != null)
                ActiveTableEditor.SelectAllCells();
            else
                ActiveSqlEditor?.SelectAll();
        }

        // ----------------------------------------------------------------
        // Record menu
        // ----------------------------------------------------------------

        private void insertRecordMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No table is currently open.", "Insert Record",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.InsertRecord();
        }

        private void deleteRecordMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No table is currently open.", "Delete Record",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.DeleteCurrentRecord();
        }

        // ----------------------------------------------------------------
        // Table menu
        // ----------------------------------------------------------------

        private void infoStructureMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No table is currently open.", "Info Structure",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.ShowInfoStructure();
        }

        private void modifyStructureMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No table is currently open.", "Modify Structure",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.ModifyStructure();
        }

        private void tableRebuildMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No table is currently open.", "Table Rebuild",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.RebuildTable();
        }

        // ----------------------------------------------------------------
        // SQL menu
        // ----------------------------------------------------------------

        private void runSqlMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveSqlEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No SQL file is currently open.", "Run SQL",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.RunSql(ResolveSqlBaseDirectory());
        }

        /// <summary>
        /// Resolves the base directory used to look up bare table names
        /// referenced in a SQL script: the directory of the first open
        /// <see cref="TableEditorForm"/>, if any, else the current working
        /// directory.
        /// </summary>
        private string ResolveSqlBaseDirectory()
        {
            foreach (Form child in MdiChildren)
            {
                var tableEditor = child as TableEditorForm;
                if (tableEditor?.TableFilePath != null)
                    return Path.GetDirectoryName(tableEditor.TableFilePath);
            }

            return Directory.GetCurrentDirectory();
        }

        // ----------------------------------------------------------------
        // Help menu
        // ----------------------------------------------------------------

        private void helpMenuItemHelp_Click(object sender, EventArgs e)
        {

            var contribs = new List<string>()
            {
                "Javier Garcia (kymuweb)",
                "Patrick Sims (medilinkps)",
            };

            var helpMsg = "Paradox-Reader and associated projects by:\r\n";
            foreach (var contrib in contribs)
            {
                helpMsg += $"\r\n{contrib}";
            }
            helpMsg += "\r\n\r\nParadox-Reader is licensed under the MIT License.\r\n\r\n" +
                "For more information, visit:\r\nhttps://github.com/kymuweb/Paradox-Reader";

            MessageBox.Show(this, helpMsg, "Help",
                MessageBoxButtons.OK, MessageBoxIcon.Information);
        }
    }
}
