using System;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using ParadoxReader;

namespace ParadoxDesktop
{
    /// <summary>
    /// Modal dialog that runs a <see cref="TableRebuilder"/> rebuild operation on a
    /// background thread while displaying two progress bars: <see cref="overallProgressBar"/>
    /// tracks the whole operation and only ever completes once from 0 to 100, while
    /// <see cref="stageProgressBar"/> tracks the current <see cref="RebuildStage"/> only and
    /// resets back to 0 every time the stage changes. Supports cooperative cancellation via
    /// the Cancel button/close box, which requests cancellation of the underlying rebuild
    /// through a <see cref="CancellationTokenSource"/>.
    /// </summary>
    public partial class RebuildProgressForm : Form
    {
        private readonly Func<IProgress<TableRebuildProgress>, CancellationToken, TableRebuildResult> operation;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private RebuildStage? lastStage;
        private bool cancelRequested;
        private bool completed;

        /// <summary>The result of the rebuild once it completes successfully. Null if cancelled or failed.</summary>
        public TableRebuildResult Result { get; private set; }

        /// <summary>The exception thrown by the rebuild, if it failed for a reason other than cancellation.</summary>
        public Exception Error { get; private set; }

        /// <summary>True if the user cancelled the rebuild before it finished.</summary>
        public bool WasCancelled { get; private set; }

        private RebuildProgressForm(string title, Func<IProgress<TableRebuildProgress>, CancellationToken, TableRebuildResult> operation)
        {
            InitializeComponent();
            this.operation = operation;
            if (!string.IsNullOrEmpty(title))
                Text = title;
        }

        /// <summary>
        /// Shows the progress dialog and synchronously runs <paramref name="operation"/> on a
        /// background thread, marshalling progress updates back to the UI thread. Blocks the
        /// caller (via <see cref="Form.ShowDialog(IWin32Window)"/>'s modal message loop) until
        /// the operation completes, fails, or the user cancels it.
        /// </summary>
        /// <param name="owner">Owner window for the modal dialog.</param>
        /// <param name="title">Dialog title (e.g. "Rebuilding Table..." or "Modifying Structure...").</param>
        /// <param name="operation">
        /// Callback that performs the actual rebuild, given a progress reporter and a
        /// cancellation token to pass through to <see cref="TableRebuilder"/>.
        /// </param>
        public static RebuildProgressForm RunModal(IWin32Window owner, string title,
            Func<IProgress<TableRebuildProgress>, CancellationToken, TableRebuildResult> operation)
        {
            using (var form = new RebuildProgressForm(title, operation))
            {
                form.ShowDialog(owner);
                return form;
            }
        }

        protected override void OnShown(EventArgs e)
        {
            base.OnShown(e);
            StartOperation();
        }

        private void StartOperation()
        {
            var progress = new Progress<TableRebuildProgress>(OnProgressReported);
            var token = cancellationTokenSource.Token;

            Task.Run(() =>
            {
                try
                {
                    var result = operation(progress, token);
                    BeginInvoke((Action)(() => OnOperationCompleted(result, null, false)));
                }
                catch (OperationCanceledException)
                {
                    BeginInvoke((Action)(() => OnOperationCompleted(null, null, true)));
                }
                catch (Exception ex)
                {
                    BeginInvoke((Action)(() => OnOperationCompleted(null, ex, false)));
                }
            });
        }

        private void OnProgressReported(TableRebuildProgress p)
        {
            if (completed) return;

            if (lastStage != p.Stage)
            {
                lastStage = p.Stage;
                stageProgressBar.Value = 0;
            }

            overallLabel.Text = $"Overall progress ({p.StageNumber} of {p.TotalStages}: {TableRebuildProgress.GetStageDisplayName(p.Stage)})";
            overallProgressBar.Value = Clamp(p.OverallPercent);
            stageLabel.Text = TableRebuildProgress.GetStageDisplayName(p.Stage);
            stageProgressBar.Value = Clamp(p.StagePercent);
            if (!string.IsNullOrEmpty(p.Message))
                statusLabel.Text = p.Message;
        }

        private static int Clamp(double percent)
        {
            if (double.IsNaN(percent)) return 0;
            if (percent < 0) return 0;
            if (percent > 100) return 100;
            return (int)percent;
        }

        private void OnOperationCompleted(TableRebuildResult result, Exception error, bool wasCancelled)
        {
            completed = true;
            Result = result;
            Error = error;
            WasCancelled = wasCancelled;

            if (result != null)
            {
                overallProgressBar.Value = 100;
                stageProgressBar.Value = 100;
            }

            Close();
        }

        private void cancelButton_Click(object sender, EventArgs e)
        {
            RequestCancel();
        }

        private void RequestCancel()
        {
            if (completed || cancelRequested) return;

            cancelRequested = true;
            cancelButton.Enabled = false;
            cancelButton.Text = "Cancelling...";
            statusLabel.Text = "Cancelling, please wait...";
            cancellationTokenSource.Cancel();
        }

        private void RebuildProgressForm_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (!completed)
            {
                // Don't let the window close (via Alt+F4/close box) until the
                // background operation has actually finished/observed cancellation;
                // just request cancellation instead and keep the dialog open.
                e.Cancel = true;
                RequestCancel();
            }
        }
    }
}
