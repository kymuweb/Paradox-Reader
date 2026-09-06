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
            this.openMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.saveAsMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.exportMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.fileMenuSeparator1 = new System.Windows.Forms.ToolStripSeparator();
            this.exitMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.editModeMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.modifyMemoMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.tableMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.infoStructureMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.tableRebuildMenuItem = new System.Windows.Forms.ToolStripMenuItem();
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
            this.tableMenuItem,
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
            this.openMenuItem,
            this.saveAsMenuItem,
            this.exportMenuItem,
            this.fileMenuSeparator1,
            this.exitMenuItem});
            this.fileMenuItem.Name = "fileMenuItem";
            this.fileMenuItem.Size = new System.Drawing.Size(37, 20);
            this.fileMenuItem.Text = "&File";
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
            this.editModeMenuItem,
            this.modifyMemoMenuItem});
            this.editMenuItem.Name = "editMenuItem";
            this.editMenuItem.Size = new System.Drawing.Size(39, 20);
            this.editMenuItem.Text = "&Edit";
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
            // tableMenuItem
            //
            this.tableMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.infoStructureMenuItem,
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
            // tableRebuildMenuItem
            //
            this.tableRebuildMenuItem.Name = "tableRebuildMenuItem";
            this.tableRebuildMenuItem.Size = new System.Drawing.Size(180, 22);
            this.tableRebuildMenuItem.Text = "Table &Rebuild";
            this.tableRebuildMenuItem.Click += new System.EventHandler(this.tableRebuildMenuItem_Click);
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
            this.menuStrip.ResumeLayout(false);
            this.menuStrip.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menuStrip;
        private System.Windows.Forms.ToolStripMenuItem fileMenuItem;
        private System.Windows.Forms.ToolStripMenuItem openMenuItem;
        private System.Windows.Forms.ToolStripMenuItem saveAsMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exportMenuItem;
        private System.Windows.Forms.ToolStripSeparator fileMenuSeparator1;
        private System.Windows.Forms.ToolStripMenuItem exitMenuItem;
        private System.Windows.Forms.ToolStripMenuItem editMenuItem;
        private System.Windows.Forms.ToolStripMenuItem editModeMenuItem;
        private System.Windows.Forms.ToolStripMenuItem modifyMemoMenuItem;
        private System.Windows.Forms.ToolStripMenuItem tableMenuItem;
        private System.Windows.Forms.ToolStripMenuItem infoStructureMenuItem;
        private System.Windows.Forms.ToolStripMenuItem tableRebuildMenuItem;
        private System.Windows.Forms.ToolStripMenuItem helpMenuItem;
        private System.Windows.Forms.ToolStripMenuItem helpMenuItemHelp;
        private System.Windows.Forms.OpenFileDialog openFileDialog;
    }
}

