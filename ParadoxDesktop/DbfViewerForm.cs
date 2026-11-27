using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// Read-only MDI child that displays a dBase (.DBF) table and can export
    /// it to CSV or convert it to a new Paradox (.DB) table without indexes.
    /// </summary>
    public class DbfViewerForm : Form
    {
        private readonly DataGridView dataGridView;
        private readonly StatusStrip statusStrip;
        private readonly ToolStripStatusLabel statusLabel;
        private readonly DbfReader reader;
        private readonly DataTable table;

        public string FilePath => reader.FilePath;

        public DbfViewerForm(string dbfFilePath)
        {
            reader = new DbfReader(dbfFilePath);
            table = reader.ToDataTable();

            Text = Path.GetFileName(dbfFilePath) + " (read-only)";
            Width = 800;
            Height = 500;

            dataGridView = new DataGridView
            {
                Dock = DockStyle.Fill,
                ReadOnly = true,
                AllowUserToAddRows = false,
                AllowUserToDeleteRows = false,
                DataSource = table,
            };
            statusLabel = new ToolStripStatusLabel
            {
                Text = string.Format("{0} row(s), {1} field(s). DBF tables are read-only.", table.Rows.Count, table.Columns.Count)
            };
            statusStrip = new StatusStrip();
            statusStrip.Items.Add(statusLabel);

            Controls.Add(dataGridView);
            Controls.Add(statusStrip);
        }

        public void Copy()
        {
            var content = dataGridView.GetClipboardContent();
            if (content != null) Clipboard.SetDataObject(content);
        }

        public void SelectAll() => dataGridView.SelectAll();

        public void ExportCsv()
        {
            using (var dlg = new SaveFileDialog())
            {
                dlg.Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*";
                dlg.DefaultExt = "csv";
                dlg.FileName = Path.GetFileNameWithoutExtension(FilePath) + ".csv";
                if (dlg.ShowDialog(this) != DialogResult.OK) return;

                try
                {
                    using (var writer = new StreamWriter(dlg.FileName, false, Encoding.UTF8))
                    {
                        writer.WriteLine(string.Join(",", table.Columns.Cast<DataColumn>().Select(c => CsvEscape(c.ColumnName)).ToArray()));
                        foreach (DataRow row in table.Rows)
                        {
                            var fields = new string[table.Columns.Count];
                            for (int i = 0; i < fields.Length; i++)
                                fields[i] = CsvEscape(FormatValue(row[i]));
                            writer.WriteLine(string.Join(",", fields));
                        }
                    }
                    MessageBox.Show(this, "Exported to:\r\n" + dlg.FileName, "Export",
                        MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, "Export failed:\r\n" + ex.Message, "Export",
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        /// <summary>Converts the DBF into a new Paradox table (no primary key or secondary indexes).</summary>
        public void SaveAsParadox()
        {
            using (var dlg = new SaveFileDialog())
            {
                dlg.Filter = "Paradox tables (*.db)|*.db";
                dlg.DefaultExt = "db";
                dlg.FileName = Path.GetFileNameWithoutExtension(FilePath) + ".db";
                if (dlg.ShowDialog(this) != DialogResult.OK) return;

                try
                {
                    var schema = new TableSchemaDefinition { TableName = Path.GetFileNameWithoutExtension(dlg.FileName) };
                    var converters = new Func<object, object>[reader.Fields.Count];
                    for (int i = 0; i < reader.Fields.Count; i++)
                    {
                        var f = reader.Fields[i];
                        var t = f.ClrType;
                        string name = table.Columns[i].ColumnName;
                        if (name.Length > 25) name = name.Substring(0, 25);

                        if (t == typeof(int))
                        {
                            schema.Fields.Add(new TableFieldDefinition(name, ParadoxFieldTypes.Long, 4, false));
                            converters[i] = v => v;
                        }
                        else if (t == typeof(long) || t == typeof(decimal) || t == typeof(double))
                        {
                            schema.Fields.Add(new TableFieldDefinition(name, ParadoxFieldTypes.Number, 8, false));
                            converters[i] = v => v == null ? null : (object)Convert.ToDouble(v);
                        }
                        else if (t == typeof(DateTime))
                        {
                            bool hasTime = f.Type == 'T' || f.Type == '@';
                            schema.Fields.Add(hasTime
                                ? new TableFieldDefinition(name, ParadoxFieldTypes.Timestamp, 8, false)
                                : new TableFieldDefinition(name, ParadoxFieldTypes.Date, 4, false));
                            converters[i] = v => v;
                        }
                        else if (t == typeof(bool))
                        {
                            schema.Fields.Add(new TableFieldDefinition(name, ParadoxFieldTypes.Logical, 1, false));
                            converters[i] = v => v;
                        }
                        else
                        {
                            int size = Math.Max(1, Math.Min(255, f.Type == 'C' ? f.Length : 255));
                            schema.Fields.Add(new TableFieldDefinition(name, ParadoxFieldTypes.Alpha, (byte)size, false));
                            converters[i] = v =>
                            {
                                var s = v as string;
                                return s != null && s.Length > size ? s.Substring(0, size) : s;
                            };
                        }
                    }

                    TableCreator.CreateNew(dlg.FileName, schema);
                    using (var target = new ParadoxTableFile(dlg.FileName))
                    {
                        target.AppendRecords(table.Rows.Cast<DataRow>().Select(row =>
                        {
                            var values = new object[converters.Length];
                            for (int i = 0; i < values.Length; i++)
                                values[i] = row[i] == DBNull.Value ? null : converters[i](row[i]);
                            return values;
                        }));
                    }

                    MessageBox.Show(this, "Paradox table saved as:\r\n" + dlg.FileName, "Save As",
                        MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(this, "Save As failed:\r\n" + ex.Message, "Save As",
                        MessageBoxButtons.OK, MessageBoxIcon.Error);
                }
            }
        }

        private static string FormatValue(object value)
        {
            if (value == null || value == DBNull.Value) return string.Empty;
            if (value is DateTime dt)
                return dt.TimeOfDay == TimeSpan.Zero ? dt.ToString("yyyy-MM-dd") : dt.ToString("yyyy-MM-dd HH:mm:ss");
            return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
        }

        private static string CsvEscape(string value)
        {
            if (value == null) return string.Empty;
            if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            return value;
        }
    }
}
