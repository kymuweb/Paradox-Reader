namespace ParadoxDesktop
{
    partial class ParadoxDesktopMainForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.menuStrip = new System.Windows.Forms.MenuStrip();
            this.fileMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.newMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.newTableMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.newSqlFileMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.fileMenuSeparator2 = new System.Windows.Forms.ToolStripSeparator();
            this.openMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.saveAsMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.exportMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.fileMenuSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.exitMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.undoMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.redoMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editMenuSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.cutMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.copyMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.pasteMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editMenuSeparator2 = new System.Windows.Forms.ToolStripSeparator();
            this.selectAllMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editMenuSeparator3 = new System.Windows.Forms.ToolStripSeparator();
            this.editModeMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.modifyMemoMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.recordMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.insertRecordMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.deleteRecordMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.tableMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.infoStructureMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.modifyStructureMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.tableRebuildMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.sqlMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.runSqlMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.helpMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.helpMenuItemHelp = new System.Windows.Forms.ToolStripMenuItem();
            this.openFileDialog = new System.Windows.Forms.OpenFileDialog();
            this.menuStrip.SuspendLayout();
            this.SuspendLayout();
            //
            // menuStrip
            //
            this.menuStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fileMenuItem,
            this.editMenuItem,
            this.recordMenuItem,
            this.tableMenuItem,
            this.sqlMenuItem,
            this.helpMenuItem});
            this.menuStrip.Location = new System.Drawing.Point(0, 0);
            this.menuStrip.Name = "menuStrip";
            this.menuStrip.Size = new System.Drawing.Size(800, 24);
            this.menuStrip.TabIndex = 0;
            this.menuStrip.Text = "menuStrip";
            //
            // fileMenuItem
            //
            this.fileMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.newMenuItem,
            this.fileMenuSeparator2,
            this.openMenuItem,
            this.saveAsMenuItem,
            this.exportMenuItem,
            this.fileMenuSeparator1,
            this.exitMenuItem});
            this.fileMenuItem.Name = "fileMenuItem";
            this.fileMenuItem.Size = new System.Drawing.Size(37, 20);
            this.fileMenuItem.Text = "&File";
            //
            // newMenuItem
            //
            this.newMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.newTableMenuItem,
            this.newSqlFileMenuItem});
            this.newMenuItem.Name = "newMenuItem";
            this.newMenuItem.Size = new System.Drawing.Size(180, 22);
            this.newMenuItem.Text = "&New";
            //
            // newTableMenuItem
            //
            this.newTableMenuItem.Name = "newTableMenuItem";
            this.newTableMenuItem.Size = new System.Drawing.Size(180, 22);
            this.newTableMenuItem.Text = "&Table...";
            this.newTableMenuItem.Click += new System.EventHandler(this.newTableMenuItem_Click);
            //
            // newSqlFileMenuItem
            //
            this.newSqlFileMenuItem.Name = "newSqlFileMenuItem";
            this.newSqlFileMenuItem.Size = new System.Drawing.Size(180, 22);
            this.newSqlFileMenuItem.Text = "&SQL File";
            this.newSqlFileMenuItem.Click += new System.EventHandler(this.newSqlFileMenuItem_Click);
            //
            // fileMenuSeparator2
            //
            this.fileMenuSeparator2.Name = "fileMenuSeparator2";
            this.fileMenuSeparator2.Size = new System.Drawing.Size(177, 6);
            //
            // openMenuItem
            //
            this.openMenuItem.Name = "openMenuItem";
            this.openMenuItem.Size = new System.Drawing.Size(180, 22);
            this.openMenuItem.Text = "&Open...";
            this.openMenuItem.Click += new System.EventHandler(this.openMenuItem_Click);
            //
            // saveAsMenuItem
            //
            this.saveAsMenuItem.Name = "saveAsMenuItem";
            this.saveAsMenuItem.Size = new System.Drawing.Size(180, 22);
            this.saveAsMenuItem.Text = "Save &As...";
            this.saveAsMenuItem.Click += new System.EventHandler(this.saveAsMenuItem_Click);
            //
            // exportMenuItem
            //
            this.exportMenuItem.Name = "exportMenuItem";
            this.exportMenuItem.Size = new System.Drawing.Size(180, 22);
            this.exportMenuItem.Text = "&Export (CSV)...";
            this.exportMenuItem.Click += new System.EventHandler(this.exportMenuItem_Click);
            //
            // fileMenuSeparator1
            //
            this.fileMenuSeparator1.Name = "fileMenuSeparator1";
            this.fileMenuSeparator1.Size = new System.Drawing.Size(177, 6);
            //
            // exitMenuItem
            //
            this.exitMenuItem.Name = "exitMenuItem";
            this.exitMenuItem.Size = new System.Drawing.Size(180, 22);
            this.exitMenuItem.Text = "E&xit";
            this.exitMenuItem.Click += new System.EventHandler(this.exitMenuItem_Click);
            //
            // editMenuItem
            //
            this.editMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.undoMenuItem,
            this.redoMenuItem,
            this.editMenuSeparator1,
            this.cutMenuItem,
            this.copyMenuItem,
            this.pasteMenuItem,
            this.editMenuSeparator2,
            this.selectAllMenuItem,
            this.editMenuSeparator3,
            this.editModeMenuItem,
            this.modifyMemoMenuItem});
            this.editMenuItem.Name = "editMenuItem";
            this.editMenuItem.Size = new System.Drawing.Size(39, 20);
            this.editMenuItem.Text = "&Edit";
            //
            // undoMenuItem
            //
            this.undoMenuItem.Name = "undoMenuItem";
            this.undoMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Z)));
            this.undoMenuItem.Size = new System.Drawing.Size(220, 22);
            this.undoMenuItem.Text = "&Undo";
            this.undoMenuItem.Click += new System.EventHandler(this.undoMenuItem_Click);
            //
            // redoMenuItem
            //
            this.redoMenuItem.Name = "redoMenuItem";
            this.redoMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Y)));
            this.redoMenuItem.Size = new System.Drawing.Size(220, 22);
            this.redoMenuItem.Text = "&Redo";
            this.redoMenuItem.Click += new System.EventHandler(this.redoMenuItem_Click);
            //
            // editMenuSeparator1
            //
            this.editMenuSeparator1.Name = "editMenuSeparator1";
            this.editMenuSeparator1.Size = new System.Drawing.Size(217, 6);
            //
            // cutMenuItem
            //
            this.cutMenuItem.Name = "cutMenuItem";
            this.cutMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.X)));
            this.cutMenuItem.Size = new System.Drawing.Size(220, 22);
            this.cutMenuItem.Text = "Cu&t";
            this.cutMenuItem.Click += new System.EventHandler(this.cutMenuItem_Click);
            //
            // copyMenuItem
            //
            this.copyMenuItem.Name = "copyMenuItem";
            this.copyMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.C)));
            this.copyMenuItem.Size = new System.Drawing.Size(220, 22);
            this.copyMenuItem.Text = "&Copy";
            this.copyMenuItem.Click += new System.EventHandler(this.copyMenuItem_Click);
            //
            // pasteMenuItem
            //
            this.pasteMenuItem.Name = "pasteMenuItem";
            this.pasteMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.V)));
            this.pasteMenuItem.Size = new System.Drawing.Size(220, 22);
            this.pasteMenuItem.Text = "&Paste";
            this.pasteMenuItem.Click += new System.EventHandler(this.pasteMenuItem_Click);
            //
            // editMenuSeparator2
            //
            this.editMenuSeparator2.Name = "editMenuSeparator2";
            this.editMenuSeparator2.Size = new System.Drawing.Size(217, 6);
            //
            // selectAllMenuItem
            //
            this.selectAllMenuItem.Name = "selectAllMenuItem";
            this.selectAllMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.A)));
            this.selectAllMenuItem.Size = new System.Drawing.Size(220, 22);
            this.selectAllMenuItem.Text = "Select &All";
            this.selectAllMenuItem.Click += new System.EventHandler(this.selectAllMenuItem_Click);
            //
            // editMenuSeparator3
            //
            this.editMenuSeparator3.Name = "editMenuSeparator3";
            this.editMenuSeparator3.Size = new System.Drawing.Size(217, 6);
            //
            // editModeMenuItem
            //
            this.editModeMenuItem.Name = "editModeMenuItem";
            this.editModeMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F9;
            this.editModeMenuItem.Size = new System.Drawing.Size(220, 22);
            this.editModeMenuItem.Text = "Edit &Mode";
            this.editModeMenuItem.Click += new System.EventHandler(this.editModeMenuItem_Click);
            //
            // modifyMemoMenuItem
            //
            this.modifyMemoMenuItem.Name = "modifyMemoMenuItem";
            this.modifyMemoMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F2;
            this.modifyMemoMenuItem.Size = new System.Drawing.Size(220, 22);
            this.modifyMemoMenuItem.Text = "Modify &Memo/Blob";
            this.modifyMemoMenuItem.Click += new System.EventHandler(this.modifyMemoMenuItem_Click);
            //
            // recordMenuItem
            //
            this.recordMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.insertRecordMenuItem,
            this.deleteRecordMenuItem});
            this.recordMenuItem.Name = "recordMenuItem";
            this.recordMenuItem.Size = new System.Drawing.Size(58, 20);
            this.recordMenuItem.Text = "&Record";
            //
            // insertRecordMenuItem
            //
            this.insertRecordMenuItem.Name = "insertRecordMenuItem";
            this.insertRecordMenuItem.ShortcutKeys = System.Windows.Forms.Keys.Insert;
            this.insertRecordMenuItem.Size = new System.Drawing.Size(220, 22);
            this.insertRecordMenuItem.Text = "&Insert";
            this.insertRecordMenuItem.Click += new System.EventHandler(this.insertRecordMenuItem_Click);
            //
            // deleteRecordMenuItem
            //
            this.deleteRecordMenuItem.Name = "deleteRecordMenuItem";
            this.deleteRecordMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Delete)));
            this.deleteRecordMenuItem.Size = new System.Drawing.Size(220, 22);
            this.deleteRecordMenuItem.Text = "&Delete";
            this.deleteRecordMenuItem.Click += new System.EventHandler(this.deleteRecordMenuItem_Click);
            //
            // tableMenuItem
            //
            this.tableMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.infoStructureMenuItem,
            this.modifyStructureMenuItem,
            this.tableRebuildMenuItem});
            this.tableMenuItem.Name = "tableMenuItem";
            this.tableMenuItem.Size = new System.Drawing.Size(50, 20);
            this.tableMenuItem.Text = "&Table";
            //
            // infoStructureMenuItem
            //
            this.infoStructureMenuItem.Name = "infoStructureMenuItem";
            this.infoStructureMenuItem.Size = new System.Drawing.Size(180, 22);
            this.infoStructureMenuItem.Text = "&Info Structure...";
            this.infoStructureMenuItem.Click += new System.EventHandler(this.infoStructureMenuItem_Click);
            //
            // modifyStructureMenuItem
            //
            this.modifyStructureMenuItem.Name = "modifyStructureMenuItem";
            this.modifyStructureMenuItem.Size = new System.Drawing.Size(180, 22);
            this.modifyStructureMenuItem.Text = "&Modify Structure...";
            this.modifyStructureMenuItem.Click += new System.EventHandler(this.modifyStructureMenuItem_Click);
            //
            // tableRebuildMenuItem
            //
            this.tableRebuildMenuItem.Name = "tableRebuildMenuItem";
            this.tableRebuildMenuItem.Size = new System.Drawing.Size(180, 22);
            this.tableRebuildMenuItem.Text = "Table &Rebuild";
            this.tableRebuildMenuItem.Click += new System.EventHandler(this.tableRebuildMenuItem_Click);
            //
            // sqlMenuItem
            //
            this.sqlMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.runSqlMenuItem});
            this.sqlMenuItem.Name = "sqlMenuItem";
            this.sqlMenuItem.Size = new System.Drawing.Size(42, 20);
            this.sqlMenuItem.Text = "&SQL";
            //
            // runSqlMenuItem
            //
            this.runSqlMenuItem.Name = "runSqlMenuItem";
            this.runSqlMenuItem.ShortcutKeys = System.Windows.Forms.Keys.F8;
            this.runSqlMenuItem.Size = new System.Drawing.Size(180, 22);
            this.runSqlMenuItem.Text = "&Run SQL";
            this.runSqlMenuItem.Click += new System.EventHandler(this.runSqlMenuItem_Click);
            //
            // helpMenuItem
            //
            this.helpMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.helpMenuItemHelp});
            this.helpMenuItem.Name = "helpMenuItem";
            this.helpMenuItem.Size = new System.Drawing.Size(44, 20);
            this.helpMenuItem.Text = "&Help";
            //
            // helpMenuItemHelp
            //
            this.helpMenuItemHelp.Name = "helpMenuItemHelp";
            this.helpMenuItemHelp.Size = new System.Drawing.Size(180, 22);
            this.helpMenuItemHelp.Text = "&Help...";
            this.helpMenuItemHelp.Click += new System.EventHandler(this.helpMenuItemHelp_Click);
            //
            // openFileDialog
            //
            this.openFileDialog.DefaultExt = "db";
            this.openFileDialog.Filter = "Paradox tables (*.db)|*.db|All files (*.*)|*.*";
            this.openFileDialog.Title = "Open Paradox Table";
            //
            // ParadoxDesktopMainForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.IsMdiContainer = true;
            this.MainMenuStrip = this.menuStrip;
            this.Controls.Add(this.menuStrip);
            this.Name = "ParadoxDesktopMainForm";
            this.Text = "Paradox Desktop";
            this.MdiChildActivate += new System.EventHandler(this.ParadoxDesktopMainForm_MdiChildActivate);
            this.menuStrip.ResumeLayout(false);
            this.menuStrip.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menuStrip;
        private System.Windows.Forms.ToolStripMenuItem newTableMenuItem;
        private System.Windows.Forms.ToolStripMenuItem modifyStructureMenuItem;
        private System.Windows.Forms.ToolStripMenuItem fileMenuItem;
        private System.Windows.Forms.ToolStripMenuItem newMenuItem;
        private System.Windows.Forms.ToolStripMenuItem newSqlFileMenuItem;
        private System.Windows.Forms.ToolStripSeparator fileMenuSeparator2;
        private System.Windows.Forms.ToolStripMenuItem openMenuItem;
        private System.Windows.Forms.ToolStripMenuItem saveAsMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exportMenuItem;
        private System.Windows.Forms.ToolStripSeparator fileMenuSeparator1;
        private System.Windows.Forms.ToolStripMenuItem exitMenuItem;
        private System.Windows.Forms.ToolStripMenuItem editMenuItem;
        private System.Windows.Forms.ToolStripMenuItem undoMenuItem;
        private System.Windows.Forms.ToolStripMenuItem redoMenuItem;
        private System.Windows.Forms.ToolStripSeparator editMenuSeparator1;
        private System.Windows.Forms.ToolStripMenuItem cutMenuItem;
        private System.Windows.Forms.ToolStripMenuItem copyMenuItem;
        private System.Windows.Forms.ToolStripMenuItem pasteMenuItem;
        private System.Windows.Forms.ToolStripSeparator editMenuSeparator2;
        private System.Windows.Forms.ToolStripMenuItem selectAllMenuItem;
        private System.Windows.Forms.ToolStripSeparator editMenuSeparator3;
        private System.Windows.Forms.ToolStripMenuItem editModeMenuItem;
        private System.Windows.Forms.ToolStripMenuItem modifyMemoMenuItem;
        private System.Windows.Forms.ToolStripMenuItem recordMenuItem;
        private System.Windows.Forms.ToolStripMenuItem insertRecordMenuItem;
        private System.Windows.Forms.ToolStripMenuItem deleteRecordMenuItem;
        private System.Windows.Forms.ToolStripMenuItem tableMenuItem;
        private System.Windows.Forms.ToolStripMenuItem infoStructureMenuItem;
        private System.Windows.Forms.ToolStripMenuItem tableRebuildMenuItem;
        private System.Windows.Forms.ToolStripMenuItem sqlMenuItem;
        private System.Windows.Forms.ToolStripMenuItem runSqlMenuItem;
        private System.Windows.Forms.ToolStripMenuItem helpMenuItem;
        private System.Windows.Forms.ToolStripMenuItem helpMenuItemHelp;
        private System.Windows.Forms.OpenFileDialog openFileDialog;
    }
}

