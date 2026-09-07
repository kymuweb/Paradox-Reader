using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Windows.Forms;
using ParadoxReader.Sql;

namespace ParadoxDesktop
{
    /// <summary>
    /// Floating MDI child window providing a simple SQL script editor and
    /// runner ("SQL" - Run SQL, F8) against Paradox tables via the
    /// <see cref="ParadoxReader.Sql"/> ADO.NET-style wrapper
    /// (<see cref="ParadoxConnection"/>/<see cref="ParadoxCommand"/>).
    /// Multiple statements (separated by blank lines or ';') are executed in
    /// sequence; the last SELECT's results are shown in the grid, and a
    /// summary/errors are reported in the status bar.
    /// </summary>
    public partial class SqlEditorForm : Form
    {
        private ParadoxConnection connection;

        public SqlEditorForm()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Runs the SQL script currently in the editor (Run SQL, F8). Bare
        /// table names are resolved relative to <paramref name="baseDirectory"/>
        /// if provided (typically the directory of the currently/most-recently
        /// open table), else the current working directory.
        /// </summary>
        public void RunSql(string baseDirectory)
        {
            string script = sqlTextBox.Text;
            if (string.IsNullOrEmpty(script) || script.Trim().Length == 0)
            {
                statusLabel.Text = "Nothing to run.";
                return;
            }

            var statements = SplitStatements(script);
            if (statements.Count == 0)
            {
                statusLabel.Text = "Nothing to run.";
                return;
            }

            resultsGridView.DataSource = null;

            if (connection == null)
                connection = new ParadoxConnection();
            connection.ConnectionString = baseDirectory ?? Directory.GetCurrentDirectory();

            try
            {
                if (connection.State != ConnectionState.Open)
                    connection.Open();

                int totalAffected = 0;
                int selectCount = 0;
                DataTable lastResultTable = null;

                foreach (var statement in statements)
                {
                    using (var cmd = (ParadoxCommand)connection.CreateCommand())
                    {
                        cmd.CommandText = statement;

                        if (IsSelect(statement))
                        {
                            using (var reader = cmd.ExecuteReader())
                            {
                                lastResultTable = new DataTable();
                                lastResultTable.Load(reader);
                            }
                            selectCount++;
                        }
                        else
                        {
                            totalAffected += cmd.ExecuteNonQuery();
                        }
                    }
                }

                if (lastResultTable != null)
                {
                    resultsGridView.DataSource = lastResultTable;
                    statusLabel.Text = string.Format("{0} statement(s) executed. Last SELECT returned {1} row(s).",
                        statements.Count, lastResultTable.Rows.Count);
                }
                else
                {
                    statusLabel.Text = string.Format("{0} statement(s) executed. {1} row(s) affected.",
                        statements.Count, totalAffected);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "SQL run failed:\r\n" + ex.Message, "Run SQL",
                    MessageBoxButtons.OK, MessageBoxIcon.Error);
                statusLabel.Text = "Run failed: " + ex.Message;
            }
        }

        /// <summary>Splits a SQL script into individual statements on ';' terminators, ignoring blank entries.</summary>
        private static System.Collections.Generic.List<string> SplitStatements(string script)
        {
            return script.Split(';')
                .Select(s => s.Trim())
                .Where(s => s.Length > 0)
                .ToList();
        }

        private static bool IsSelect(string statement) =>
            statement.TrimStart().StartsWith("select", StringComparison.OrdinalIgnoreCase);

        // ----------------------------------------------------------------
        // Edit menu (Cut/Copy/Paste/Select All)
        // ----------------------------------------------------------------

        public void Cut() => sqlTextBox.Cut();

        public void Copy() => sqlTextBox.Copy();

        public void Paste() => sqlTextBox.Paste();

        public new void SelectAll() => sqlTextBox.SelectAll();
    }
}
