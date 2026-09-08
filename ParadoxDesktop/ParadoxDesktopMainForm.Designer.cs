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
            this.recordMenuSeparator1 = new System.Windows.Forms.ToolStripSeparator();
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
            this.mainToolStrip = new System.Windows.Forms.ToolStrip();
            this.undoToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe1 = new System.Windows.Forms.ToolStripLabel();
            this.redoToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe2 = new System.Windows.Forms.ToolStripLabel();
            this.cutToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe3 = new System.Windows.Forms.ToolStripLabel();
            this.copyToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe4 = new System.Windows.Forms.ToolStripLabel();
            this.pasteToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe5 = new System.Windows.Forms.ToolStripLabel();
            this.selectAllToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe6 = new System.Windows.Forms.ToolStripLabel();
            this.editModeToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe7 = new System.Windows.Forms.ToolStripLabel();
            this.modifyMemoToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe8 = new System.Windows.Forms.ToolStripLabel();
            this.insertRecordToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe9 = new System.Windows.Forms.ToolStripLabel();
            this.deleteRecordToolButton = new System.Windows.Forms.ToolStripButton();
            this.pipe10 = new System.Windows.Forms.ToolStripLabel();
            this.runSqlToolButton = new System.Windows.Forms.ToolStripButton();
            this.menuStrip.SuspendLayout();
            this.mainToolStrip.SuspendLayout();
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
            this.newTableMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.N)));
            this.newTableMenuItem.Size = new System.Drawing.Size(180, 22);
            this.newTableMenuItem.Text = "&Table...";
            this.newTableMenuItem.Click += new System.EventHandler(this.newTableMenuItem_Click);
            //
            // newSqlFileMenuItem
            //
            this.newSqlFileMenuItem.Name = "newSqlFileMenuItem";
            this.newSqlFileMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)(((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Shift) | System.Windows.Forms.Keys.N)));
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
            this.openMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.O)));
            this.openMenuItem.Size = new System.Drawing.Size(180, 22);
            this.openMenuItem.Text = "&Open...";
            this.openMenuItem.Click += new System.EventHandler(this.openMenuItem_Click);
            //
            // saveAsMenuItem
            //
            this.saveAsMenuItem.Name = "saveAsMenuItem";
            this.saveAsMenuItem.ShortcutKeys = ((System.Windows.Forms.Keys)(((System.Windows.Forms.Keys.Control | System.Windows.Forms.Keys.Shift) | System.Windows.Forms.Keys.S)));
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
            this.exitMenuItem.ShortcutKeys = System.Windows.Forms.Keys.Alt | System.Windows.Forms.Keys.F4;
            this.exitMenuItem.ShowShortcutKeys = true;
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
            this.selectAllMenuItem});
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
            this.deleteRecordMenuItem,
            this.recordMenuSeparator1,
            this.editModeMenuItem,
            this.modifyMemoMenuItem});
            this.recordMenuItem.Name = "recordMenuItem";
            this.recordMenuItem.Size = new System.Drawing.Size(58, 20);
            this.recordMenuItem.Text = "&Record";
            //
            // recordMenuSeparator1
            //
            this.recordMenuSeparator1.Name = "recordMenuSeparator1";
            this.recordMenuSeparator1.Size = new System.Drawing.Size(217, 6);
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
            this.helpMenuItemHelp.ShortcutKeys = System.Windows.Forms.Keys.F1;
            this.helpMenuItemHelp.Size = new System.Drawing.Size(180, 22);
            this.helpMenuItemHelp.Text = "&Help...";
            this.helpMenuItemHelp.Click += new System.EventHandler(this.helpMenuItemHelp_Click);
            //
            // openFileDialog
            //
            this.openFileDialog.DefaultExt = "db";
            this.openFileDialog.Filter = "Paradox tables and SQL scripts (*.db;*.sql)|*.db;*.sql|Paradox tables (*.db)|*.db|SQL scripts (*.sql)|*.sql|All files (*.*)|*.*";
            this.openFileDialog.Title = "Open";
            //
            // mainToolStrip
            //
            this.mainToolStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.undoToolButton,
            this.pipe1,
            this.redoToolButton,
            this.pipe2,
            this.cutToolButton,
            this.pipe3,
            this.copyToolButton,
            this.pipe4,
            this.pasteToolButton,
            this.pipe5,
            this.selectAllToolButton,
            this.pipe6,
            this.insertRecordToolButton,
            this.pipe9,
            this.deleteRecordToolButton,
            this.pipe7,
            this.editModeToolButton,
            this.pipe8,
            this.modifyMemoToolButton,
            this.pipe10,
            this.runSqlToolButton});
            this.mainToolStrip.Dock = System.Windows.Forms.DockStyle.Top;
            this.mainToolStrip.Name = "mainToolStrip";
            this.mainToolStrip.TabIndex = 1;
            this.mainToolStrip.LayoutStyle = System.Windows.Forms.ToolStripLayoutStyle.HorizontalStackWithOverflow;
            //
            // undoToolButton
            //
            this.undoToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.undoToolButton.Name = "undoToolButton";
            this.undoToolButton.Text = "Undo";
            this.undoToolButton.Click += new System.EventHandler(this.undoMenuItem_Click);
            //
            // pipe1
            //
            this.pipe1.Name = "pipe1";
            this.pipe1.Text = "|";
            //
            // redoToolButton
            //
            this.redoToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.redoToolButton.Name = "redoToolButton";
            this.redoToolButton.Text = "Redo";
            this.redoToolButton.Click += new System.EventHandler(this.redoMenuItem_Click);
            //
            // pipe2
            //
            this.pipe2.Name = "pipe2";
            this.pipe2.Text = "|";
            //
            // cutToolButton
            //
            this.cutToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.cutToolButton.Name = "cutToolButton";
            this.cutToolButton.Text = "Cut";
            this.cutToolButton.Click += new System.EventHandler(this.cutMenuItem_Click);
            //
            // pipe3
            //
            this.pipe3.Name = "pipe3";
            this.pipe3.Text = "|";
            //
            // copyToolButton
            //
            this.copyToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.copyToolButton.Name = "copyToolButton";
            this.copyToolButton.Text = "Copy";
            this.copyToolButton.Click += new System.EventHandler(this.copyMenuItem_Click);
            //
            // pipe4
            //
            this.pipe4.Name = "pipe4";
            this.pipe4.Text = "|";
            //
            // pasteToolButton
            //
            this.pasteToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.pasteToolButton.Name = "pasteToolButton";
            this.pasteToolButton.Text = "Paste";
            this.pasteToolButton.Click += new System.EventHandler(this.pasteMenuItem_Click);
            //
            // pipe5
            //
            this.pipe5.Name = "pipe5";
            this.pipe5.Text = "|";
            //
            // selectAllToolButton
            //
            this.selectAllToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.selectAllToolButton.Name = "selectAllToolButton";
            this.selectAllToolButton.Text = "Select All";
            this.selectAllToolButton.Click += new System.EventHandler(this.selectAllMenuItem_Click);
            //
            // pipe6
            //
            this.pipe6.Name = "pipe6";
            this.pipe6.Text = "|";
            //
            // editModeToolButton
            //
            this.editModeToolButton.CheckOnClick = true;
            this.editModeToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.editModeToolButton.Name = "editModeToolButton";
            this.editModeToolButton.Text = "Edit Mode";
            this.editModeToolButton.Click += new System.EventHandler(this.editModeMenuItem_Click);
            //
            // pipe7
            //
            this.pipe7.Name = "pipe7";
            this.pipe7.Text = "|";
            //
            // modifyMemoToolButton
            //
            this.modifyMemoToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.modifyMemoToolButton.Name = "modifyMemoToolButton";
            this.modifyMemoToolButton.Text = "Modify Memo/Blob";
            this.modifyMemoToolButton.Click += new System.EventHandler(this.modifyMemoMenuItem_Click);
            //
            // pipe8
            //
            this.pipe8.Name = "pipe8";
            this.pipe8.Text = "|";
            //
            // insertRecordToolButton
            //
            this.insertRecordToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.insertRecordToolButton.Name = "insertRecordToolButton";
            this.insertRecordToolButton.Text = "Insert";
            this.insertRecordToolButton.Click += new System.EventHandler(this.insertRecordMenuItem_Click);
            //
            // pipe9
            //
            this.pipe9.Name = "pipe9";
            this.pipe9.Text = "|";
            //
            // deleteRecordToolButton
            //
            this.deleteRecordToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.deleteRecordToolButton.Name = "deleteRecordToolButton";
            this.deleteRecordToolButton.Text = "Delete";
            this.deleteRecordToolButton.Click += new System.EventHandler(this.deleteRecordMenuItem_Click);
            //
            // pipe10
            //
            this.pipe10.Name = "pipe10";
            this.pipe10.Text = "|";
            //
            // runSqlToolButton
            //
            this.runSqlToolButton.DisplayStyle = System.Windows.Forms.ToolStripItemDisplayStyle.Text;
            this.runSqlToolButton.Name = "runSqlToolButton";
            this.runSqlToolButton.Text = "Run SQL";
            this.runSqlToolButton.Click += new System.EventHandler(this.runSqlMenuItem_Click);
            //
            // ParadoxDesktopMainForm
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.IsMdiContainer = true;
            this.MainMenuStrip = this.menuStrip;
            this.Controls.Add(this.mainToolStrip);
            this.Controls.Add(this.menuStrip);
            this.Name = "ParadoxDesktopMainForm";
            this.Text = "Paradox Desktop";
            this.MdiChildActivate += new System.EventHandler(this.ParadoxDesktopMainForm_MdiChildActivate);
            this.menuStrip.ResumeLayout(false);
            this.menuStrip.PerformLayout();
            this.mainToolStrip.ResumeLayout(false);
            this.mainToolStrip.PerformLayout();
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
        private System.Windows.Forms.ToolStripSeparator recordMenuSeparator1;
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
        private System.Windows.Forms.ToolStrip mainToolStrip;
        private System.Windows.Forms.ToolStripButton undoToolButton;
        private System.Windows.Forms.ToolStripLabel pipe1;
        private System.Windows.Forms.ToolStripButton redoToolButton;
        private System.Windows.Forms.ToolStripLabel pipe2;
        private System.Windows.Forms.ToolStripButton cutToolButton;
        private System.Windows.Forms.ToolStripLabel pipe3;
        private System.Windows.Forms.ToolStripButton copyToolButton;
        private System.Windows.Forms.ToolStripLabel pipe4;
        private System.Windows.Forms.ToolStripButton pasteToolButton;
        private System.Windows.Forms.ToolStripLabel pipe5;
        private System.Windows.Forms.ToolStripButton selectAllToolButton;
        private System.Windows.Forms.ToolStripLabel pipe6;
        private System.Windows.Forms.ToolStripButton editModeToolButton;
        private System.Windows.Forms.ToolStripLabel pipe7;
        private System.Windows.Forms.ToolStripButton modifyMemoToolButton;
        private System.Windows.Forms.ToolStripLabel pipe8;
        private System.Windows.Forms.ToolStripButton insertRecordToolButton;
        private System.Windows.Forms.ToolStripLabel pipe9;
        private System.Windows.Forms.ToolStripButton deleteRecordToolButton;
        private System.Windows.Forms.ToolStripLabel pipe10;
        private System.Windows.Forms.ToolStripButton runSqlToolButton;
    }
}

