namespace ParadoxDesktop
{
    partial class IndexFieldsForm
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
            this.availableLabel = new System.Windows.Forms.Label();
            this.availableListBox = new System.Windows.Forms.ListBox();
            this.chosenLabel = new System.Windows.Forms.Label();
            this.chosenListBox = new System.Windows.Forms.ListBox();
            this.addButton = new System.Windows.Forms.Button();
            this.removeButton = new System.Windows.Forms.Button();
            this.moveUpButton = new System.Windows.Forms.Button();
            this.moveDownButton = new System.Windows.Forms.Button();
            this.okButton = new System.Windows.Forms.Button();
            this.cancelButton = new System.Windows.Forms.Button();
            this.SuspendLayout();
            //
            // availableLabel
            //
            this.availableLabel.AutoSize = true;
            this.availableLabel.Location = new System.Drawing.Point(12, 9);
            this.availableLabel.Name = "availableLabel";
            this.availableLabel.Size = new System.Drawing.Size(84, 13);
            this.availableLabel.TabIndex = 0;
            this.availableLabel.Text = "Available fields:";
            //
            // availableListBox
            //
            this.availableListBox.FormattingEnabled = true;
            this.availableListBox.Location = new System.Drawing.Point(12, 25);
            this.availableListBox.Name = "availableListBox";
            this.availableListBox.Size = new System.Drawing.Size(180, 212);
            this.availableListBox.TabIndex = 1;
            this.availableListBox.SelectedIndexChanged += new System.EventHandler(this.availableListBox_SelectedIndexChanged);
            //
            // chosenLabel
            //
            this.chosenLabel.AutoSize = true;
            this.chosenLabel.Location = new System.Drawing.Point(280, 9);
            this.chosenLabel.Name = "chosenLabel";
            this.chosenLabel.Size = new System.Drawing.Size(68, 13);
            this.chosenLabel.TabIndex = 4;
            this.chosenLabel.Text = "Index fields:";
            //
            // chosenListBox
            //
            this.chosenListBox.FormattingEnabled = true;
            this.chosenListBox.Location = new System.Drawing.Point(283, 25);
            this.chosenListBox.Name = "chosenListBox";
            this.chosenListBox.Size = new System.Drawing.Size(180, 212);
            this.chosenListBox.TabIndex = 5;
            this.chosenListBox.SelectedIndexChanged += new System.EventHandler(this.chosenListBox_SelectedIndexChanged);
            //
            // addButton
            //
            this.addButton.Location = new System.Drawing.Point(198, 75);
            this.addButton.Name = "addButton";
            this.addButton.Size = new System.Drawing.Size(75, 23);
            this.addButton.TabIndex = 2;
            this.addButton.Text = "Add >>";
            this.addButton.UseVisualStyleBackColor = true;
            this.addButton.Click += new System.EventHandler(this.addButton_Click);
            //
            // removeButton
            //
            this.removeButton.Location = new System.Drawing.Point(198, 104);
            this.removeButton.Name = "removeButton";
            this.removeButton.Size = new System.Drawing.Size(75, 23);
            this.removeButton.TabIndex = 3;
            this.removeButton.Text = "<< Remove";
            this.removeButton.UseVisualStyleBackColor = true;
            this.removeButton.Click += new System.EventHandler(this.removeButton_Click);
            //
            // moveUpButton
            //
            this.moveUpButton.Location = new System.Drawing.Point(469, 25);
            this.moveUpButton.Name = "moveUpButton";
            this.moveUpButton.Size = new System.Drawing.Size(60, 23);
            this.moveUpButton.TabIndex = 6;
            this.moveUpButton.Text = "Up";
            this.moveUpButton.UseVisualStyleBackColor = true;
            this.moveUpButton.Click += new System.EventHandler(this.moveUpButton_Click);
            //
            // moveDownButton
            //
            this.moveDownButton.Location = new System.Drawing.Point(469, 54);
            this.moveDownButton.Name = "moveDownButton";
            this.moveDownButton.Size = new System.Drawing.Size(60, 23);
            this.moveDownButton.TabIndex = 7;
            this.moveDownButton.Text = "Down";
            this.moveDownButton.UseVisualStyleBackColor = true;
            this.moveDownButton.Click += new System.EventHandler(this.moveDownButton_Click);
            //
            // okButton
            //
            this.okButton.DialogResult = System.Windows.Forms.DialogResult.OK;
            this.okButton.Location = new System.Drawing.Point(388, 253);
            this.okButton.Name = "okButton";
            this.okButton.Size = new System.Drawing.Size(75, 23);
            this.okButton.TabIndex = 8;
            this.okButton.Text = "OK";
            this.okButton.UseVisualStyleBackColor = true;
            this.okButton.Click += new System.EventHandler(this.okButton_Click);
            //
            // cancelButton
            //
            this.cancelButton.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.cancelButton.Location = new System.Drawing.Point(469, 253);
            this.cancelButton.Name = "cancelButton";
            this.cancelButton.Size = new System.Drawing.Size(75, 23);
            this.cancelButton.TabIndex = 9;
            this.cancelButton.Text = "Cancel";
            this.cancelButton.UseVisualStyleBackColor = true;
            //
            // IndexFieldsForm
            //
            this.AcceptButton = this.okButton;
            this.CancelButton = this.cancelButton;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(561, 288);
            this.Controls.Add(this.cancelButton);
            this.Controls.Add(this.okButton);
            this.Controls.Add(this.moveDownButton);
            this.Controls.Add(this.moveUpButton);
            this.Controls.Add(this.chosenListBox);
            this.Controls.Add(this.chosenLabel);
            this.Controls.Add(this.removeButton);
            this.Controls.Add(this.addButton);
            this.Controls.Add(this.availableListBox);
            this.Controls.Add(this.availableLabel);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "IndexFieldsForm";
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Select Index Fields";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label availableLabel;
        private System.Windows.Forms.ListBox availableListBox;
        private System.Windows.Forms.Label chosenLabel;
        private System.Windows.Forms.ListBox chosenListBox;
        private System.Windows.Forms.Button addButton;
        private System.Windows.Forms.Button removeButton;
        private System.Windows.Forms.Button moveUpButton;
        private System.Windows.Forms.Button moveDownButton;
        private System.Windows.Forms.Button okButton;
        private System.Windows.Forms.Button cancelButton;
    }
}
