using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;

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
        }

        private TableEditorForm ActiveTableEditor => ActiveMdiChild as TableEditorForm;

        // ----------------------------------------------------------------
        // File menu
        // ----------------------------------------------------------------

        private void openMenuItem_Click(object sender, EventArgs e)
        {
            if (openFileDialog.ShowDialog(this) != DialogResult.OK)
                return;

            OpenTable(openFileDialog.FileName);
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
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No table is currently open.", "Save As",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.SaveAs();
        }

        private void exportMenuItem_Click(object sender, EventArgs e)
        {
            var editor = ActiveTableEditor;
            if (editor == null)
            {
                MessageBox.Show(this, "No table is currently open.", "Export",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.ExportCsv();
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
                MessageBox.Show(this, "No table is currently open.", "Edit Mode",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            editor.ToggleEditMode();
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
        // Help menu
        // ----------------------------------------------------------------

        private void helpMenuItemHelp_Click(object sender, EventArgs e)
        {
            MessageBox.Show(this, "Help is not yet implemented.", "Help",
                MessageBoxButtons.OK, MessageBoxIcon.Information);
        }
    }
}
