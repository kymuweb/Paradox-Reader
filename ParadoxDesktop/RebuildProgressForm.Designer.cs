namespace ParadoxDesktop
{
    partial class RebuildProgressForm
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
            this.overallLabel = new System.Windows.Forms.Label();
            this.overallProgressBar = new System.Windows.Forms.ProgressBar();
            this.stageLabel = new System.Windows.Forms.Label();
            this.stageProgressBar = new System.Windows.Forms.ProgressBar();
            this.statusLabel = new System.Windows.Forms.Label();
            this.cancelButton = new System.Windows.Forms.Button();
            this.SuspendLayout();
            //
            // overallLabel
            //
            this.overallLabel.AutoSize = true;
            this.overallLabel.Location = new System.Drawing.Point(12, 9);
            this.overallLabel.Name = "overallLabel";
            this.overallLabel.Size = new System.Drawing.Size(80, 13);
            this.overallLabel.TabIndex = 0;
            this.overallLabel.Text = "Overall progress";
            //
            // overallProgressBar
            //
            this.overallProgressBar.Location = new System.Drawing.Point(12, 25);
            this.overallProgressBar.Name = "overallProgressBar";
            this.overallProgressBar.Size = new System.Drawing.Size(420, 23);
            this.overallProgressBar.TabIndex = 1;
            //
            // stageLabel
            //
            this.stageLabel.AutoSize = true;
            this.stageLabel.Location = new System.Drawing.Point(12, 61);
            this.stageLabel.Name = "stageLabel";
            this.stageLabel.Size = new System.Drawing.Size(63, 13);
            this.stageLabel.TabIndex = 2;
            this.stageLabel.Text = "Current step";
            //
            // stageProgressBar
            //
            this.stageProgressBar.Location = new System.Drawing.Point(12, 77);
            this.stageProgressBar.Name = "stageProgressBar";
            this.stageProgressBar.Size = new System.Drawing.Size(420, 23);
            this.stageProgressBar.TabIndex = 3;
            //
            // statusLabel
            //
            this.statusLabel.AutoSize = true;
            this.statusLabel.Location = new System.Drawing.Point(12, 113);
            this.statusLabel.MaximumSize = new System.Drawing.Size(420, 0);
            this.statusLabel.Name = "statusLabel";
            this.statusLabel.Size = new System.Drawing.Size(35, 13);
            this.statusLabel.TabIndex = 4;
            this.statusLabel.Text = " ";
            //
            // cancelButton
            //
            this.cancelButton.Anchor = ((System.Windows.Forms.AnchorStyles)((System.Windows.Forms.AnchorStyles.Bottom | System.Windows.Forms.AnchorStyles.Right)));
            this.cancelButton.Location = new System.Drawing.Point(357, 145);
            this.cancelButton.Name = "cancelButton";
            this.cancelButton.Size = new System.Drawing.Size(75, 23);
            this.cancelButton.TabIndex = 5;
            this.cancelButton.Text = "Cancel";
            this.cancelButton.UseVisualStyleBackColor = true;
            this.cancelButton.Click += new System.EventHandler(this.cancelButton_Click);
            //
            // RebuildProgressForm
            //
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(444, 180);
            this.Controls.Add(this.cancelButton);
            this.Controls.Add(this.statusLabel);
            this.Controls.Add(this.stageProgressBar);
            this.Controls.Add(this.stageLabel);
            this.Controls.Add(this.overallProgressBar);
            this.Controls.Add(this.overallLabel);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "RebuildProgressForm";
            this.ShowInTaskbar = false;
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "Rebuilding Table...";
            this.FormClosing += new System.Windows.Forms.FormClosingEventHandler(this.RebuildProgressForm_FormClosing);
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Label overallLabel;
        private System.Windows.Forms.ProgressBar overallProgressBar;
        private System.Windows.Forms.Label stageLabel;
        private System.Windows.Forms.ProgressBar stageProgressBar;
        private System.Windows.Forms.Label statusLabel;
        private System.Windows.Forms.Button cancelButton;
    }
}
