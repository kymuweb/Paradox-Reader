namespace ParadoxDesktop
{
    partial class TableInfoForm
    {
        private System.ComponentModel.IContainer components = null;

        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        private void InitializeComponent()
        {
            this.fieldsListView = new System.Windows.Forms.ListView();
            this.colName = new System.Windows.Forms.ColumnHeader();
            this.colType = new System.Windows.Forms.ColumnHeader();
            this.colSize = new System.Windows.Forms.ColumnHeader();
            this.colPrimaryKey = new System.Windows.Forms.ColumnHeader();
            this.summaryLabel = new System.Windows.Forms.Label();
            this.indexesLabel = new System.Windows.Forms.Label();
            this.indexesListBox = new System.Windows.Forms.ListBox();
            this.closeButton = new System.Windows.Forms.Button();
            this.SuspendLayout();
            //
            // summaryLabel
            //
            this.summaryLabel.AutoSize = true;
            this.summaryLabel.Location = new System.Drawing.Point(12, 9);
            this.summaryLabel.Name = "summaryLabel";
            this.summaryLabel.Size = new System.Drawing.Size(52, 13);
            this.summaryLabel.TabIndex = 0;
            this.summaryLabel.Text = "Summary";
            //
            // fieldsListView
            //
            this.fieldsListView.Columns.AddRange(new System.Windows.Forms.ColumnHeader[] {
            this.colName,
            this.colType,
            this.colSize,
            this.colPrimaryKey});
            this.fieldsListView.FullRowSelect = true;
            this.fieldsListView.GridLines = true;
            this.fieldsListView.Location = new System.Drawing.Point(12, 32);
            this.fieldsListView.Name = "fieldsListView";
            this.fieldsListView.Size = new System.Drawing.Size(460, 220);
            this.fieldsListView.TabIndex = 1;
            this.fieldsListView.UseCompatibleStateImageBehavior = false;
            this.fieldsListView.View = System.Windows.Forms.View.Details;
            //
            // colName
            //
            this.colName.Text = "Field Name";
            this.colName.Width = 160;
            //
            // colType
            //
            this.colType.Text = "Type";
            this.colType.Width = 120;
            //
            // colSize
            //
            this.colSize.Text = "Size";
            this.colSize.Width = 60;
            //
            // colPrimaryKey
            //
            this.colPrimaryKey.Text = "Primary Key";
            this.colPrimaryKey.Width = 90;
            //
            // indexesLabel
            //
            this.indexesLabel.AutoSize = true;
            this.indexesLabel.Location = new System.Drawing.Point(12, 262);
            this.indexesLabel.Name = "indexesLabel";
            this.indexesLabel.Size = new System.Drawing.Size(84, 13);
            this.indexesLabel.TabIndex = 2;
            this.indexesLabel.Text = "Secondary Indexes";
            //
            // indexesListBox
            //
            this.indexesListBox.FormattingEnabled = true;
            this.indexesListBox.Location = new System.Drawing.Point(12, 280);
            this.indexesListBox.Name = "indexesListBox";
            this.indexesListBox.Size = new System.Drawing.Size(460, 95);
            this.indexesListBox.TabIndex = 3;
            //
            // closeButton
            //
            this.closeButton.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.closeButton.Location = new System.Drawing.Point(397, 381);
            this.closeButton.Name = "closeButton";
            this.closeButton.Size = new System.Drawing.Size(75, 23);
            this.closeButton.TabIndex = 4;
            this.closeButton.Text = "Close";
            this.closeButton.UseVisualStyleBackColor = true;
            //
            // TableInfoForm
            //
            this.AcceptButton = this.closeButton;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(484, 416);
            this.Controls.Add(this.closeButton);
            this.Controls.Add(this.indexesListBox);
            this.Controls.Add(this.indexesLabel);
            this.Controls.Add(this.fieldsListView);
            this.Controls.Add(this.summaryLabel);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "TableInfoForm";
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Table Structure";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label summaryLabel;
        private System.Windows.Forms.ListView fieldsListView;
        private System.Windows.Forms.ColumnHeader colName;
        private System.Windows.Forms.ColumnHeader colType;
        private System.Windows.Forms.ColumnHeader colSize;
        private System.Windows.Forms.ColumnHeader colPrimaryKey;
        private System.Windows.Forms.Label indexesLabel;
        private System.Windows.Forms.ListBox indexesListBox;
        private System.Windows.Forms.Button closeButton;
    }
}
