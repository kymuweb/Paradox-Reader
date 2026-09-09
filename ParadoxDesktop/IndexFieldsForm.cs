using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;

namespace ParadoxDesktop
{
    public partial class IndexFieldsForm : Form
    {
        public List<int> SelectedFieldIndices { get; private set; } = new List<int>();

        public IndexFieldsForm(List<string> fieldNames, IEnumerable<int> initialSelection = null)
        {
            InitializeComponent();

            var initial = initialSelection != null ? new List<int>(initialSelection) : new List<int>();

            foreach (int i in initial)
                chosenListBox.Items.Add(new FieldItem(i, fieldNames[i]));

            for (int i = 0; i < fieldNames.Count; i++)
            {
                if (!initial.Contains(i))
                    availableListBox.Items.Add(new FieldItem(i, fieldNames[i]));
            }

            UpdateButtonStates();
        }

        private class FieldItem
        {
            public int Index { get; private set; }
            public string Name { get; private set; }
            public FieldItem(int index, string name) { Index = index; Name = name; }
            public override string ToString() { return Name; }
        }

        private void UpdateButtonStates()
        {
            addButton.Enabled = availableListBox.SelectedItem != null;
            removeButton.Enabled = chosenListBox.SelectedItem != null;
            moveUpButton.Enabled = chosenListBox.SelectedIndex > 0;
            moveDownButton.Enabled = chosenListBox.SelectedIndex >= 0 && chosenListBox.SelectedIndex < chosenListBox.Items.Count - 1;
        }

        private void availableListBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            UpdateButtonStates();
        }

        private void chosenListBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            UpdateButtonStates();
        }

        private void addButton_Click(object sender, EventArgs e)
        {
            var item = availableListBox.SelectedItem as FieldItem;
            if (item == null) return;

            availableListBox.Items.Remove(item);
            chosenListBox.Items.Add(item);
            UpdateButtonStates();
        }

        private void removeButton_Click(object sender, EventArgs e)
        {
            var item = chosenListBox.SelectedItem as FieldItem;
            if (item == null) return;

            chosenListBox.Items.Remove(item);
            availableListBox.Items.Add(item);
            UpdateButtonStates();
        }

        private void moveUpButton_Click(object sender, EventArgs e)
        {
            int i = chosenListBox.SelectedIndex;
            if (i <= 0) return;

            var item = chosenListBox.Items[i];
            chosenListBox.Items.RemoveAt(i);
            chosenListBox.Items.Insert(i - 1, item);
            chosenListBox.SelectedIndex = i - 1;
            UpdateButtonStates();
        }

        private void moveDownButton_Click(object sender, EventArgs e)
        {
            int i = chosenListBox.SelectedIndex;
            if (i < 0 || i >= chosenListBox.Items.Count - 1) return;

            var item = chosenListBox.Items[i];
            chosenListBox.Items.RemoveAt(i);
            chosenListBox.Items.Insert(i + 1, item);
            chosenListBox.SelectedIndex = i + 1;
            UpdateButtonStates();
        }

        private void okButton_Click(object sender, EventArgs e)
        {
            SelectedFieldIndices = chosenListBox.Items.Cast<FieldItem>().Select(f => f.Index).ToList();

            if (SelectedFieldIndices.Count == 0)
            {
                MessageBox.Show(this, "Choose at least one field for the index.", "Index Fields", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                DialogResult = DialogResult.None;
            }
        }
    }
}
