using System;
using System.IO;
using System.Text;
using System.Windows.Forms;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// Dialog for viewing/editing a single memo (MemoBLOb/FmtMemoBLOb) or
    /// binary (BLOb/OLE/Graphic/Bytes) field value, opened via F2. Memo
    /// fields are edited as plain text; binary fields are opaque and can
    /// only be replaced wholesale via "Import from file..." or inspected
    /// via "Export to file...".
    /// </summary>
    public partial class MemoEditorForm : Form
    {
        private readonly ParadoxFieldTypes fieldType;
        private readonly byte[] originalBlobInfo;
        private byte[] binaryValue;

        /// <summary>The field value to write back, in the same representation ParadoxTableFile expects (MemoValue or byte[]).</summary>
        public object ResultValue { get; private set; }

        private bool IsBinaryField =>
            fieldType == ParadoxFieldTypes.BLOb ||
            fieldType == ParadoxFieldTypes.OLE ||
            fieldType == ParadoxFieldTypes.Graphic ||
            fieldType == ParadoxFieldTypes.Bytes;

        public MemoEditorForm(string fieldName, ParadoxFieldTypes fieldType, object currentValue, bool editable)
        {
            InitializeComponent();

            this.fieldType = fieldType;
            infoLabel.Text = string.Format("Field: {0} ({1})", fieldName, fieldType);

            var memo = currentValue as MemoValue;
            if (memo != null)
            {
                originalBlobInfo = memo.BlobInfo;
                textBox.Text = memo.Text ?? string.Empty;
                exportButton.Visible = false;
            }
            else if (currentValue is byte[])
            {
                binaryValue = (byte[])((byte[])currentValue).Clone();
                textBox.Text = string.Format("<binary data: {0} byte(s)>{1}Use \"Import from file...\" to replace, or \"Export to file...\" to save a copy.",
                    binaryValue.Length, Environment.NewLine);
                textBox.ReadOnly = true;
                importButton.Visible = true;
            }
            else
            {
                textBox.Text = currentValue?.ToString() ?? string.Empty;
                exportButton.Visible = false;
                importButton.Visible = false;
            }

            if (!editable)
            {
                textBox.ReadOnly = true;
                importButton.Enabled = false;
                okButton.Enabled = false;
            }
        }

        private void importButton_Click(object sender, EventArgs e)
        {
            if (openFileDialog.ShowDialog(this) != DialogResult.OK) return;

            try
            {
                var bytes = File.ReadAllBytes(openFileDialog.FileName);

                if (IsBinaryField)
                {
                    binaryValue = bytes;
                    textBox.Text = string.Format("<binary data: {0} byte(s), imported from {1}>", bytes.Length, Path.GetFileName(openFileDialog.FileName));
                }
                else
                {
                    // Memo field: treat the imported file as text.
                    textBox.Text = Encoding.UTF8.GetString(bytes);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to import file:\r\n" + ex.Message, "Import",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void exportButton_Click(object sender, EventArgs e)
        {
            if (binaryValue == null)
            {
                MessageBox.Show(this, "No binary data to export.", "Export",
                    MessageBoxButtons.OK, MessageBoxIcon.Information);
                return;
            }

            if (saveFileDialog.ShowDialog(this) != DialogResult.OK) return;

            try
            {
                File.WriteAllBytes(saveFileDialog.FileName, binaryValue);
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to export file:\r\n" + ex.Message, "Export",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void okButton_Click(object sender, EventArgs e)
        {
            if (IsBinaryField)
            {
                ResultValue = binaryValue;
            }
            else if (fieldType == ParadoxFieldTypes.MemoBLOb || fieldType == ParadoxFieldTypes.FmtMemoBLOb)
            {
                ResultValue = new MemoValue(textBox.Text, originalBlobInfo);
            }
            else
            {
                ResultValue = textBox.Text;
            }
        }
    }
}
