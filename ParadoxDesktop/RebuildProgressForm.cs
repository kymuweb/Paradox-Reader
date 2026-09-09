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
            // NOTE: Progress<T> marshals every single Report() call to the UI thread as its
            // own queued message (via SynchronizationContext.Post). Large-table rebuilds can
            // report thousands of updates (e.g. every ~256 records), which floods the UI
            // thread's message queue; the "OnOperationCompleted" BeginInvoke then sits behind
            // that entire backlog, making the dialog appear stuck even though the background
            // operation already finished. CoalescingProgress instead keeps only the latest
            // value and schedules at most one pending UI update at a time.
            var progress = new CoalescingProgress(this, OnProgressReported);
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

        /// <summary>
        /// <see cref="IProgress{T}"/> implementation that coalesces rapid-fire reports onto the
        /// UI thread: only the most recently reported value is kept, and only one UI-thread
        /// update is ever queued at a time. Reports arriving while an update is already pending
        /// simply overwrite the pending value instead of queuing another message, so a flood of
        /// reports (e.g. one per few hundred records on a large table) can't back up the UI
        /// thread's message queue behind the operation's real completion callback.
        /// </summary>
        private sealed class CoalescingProgress : IProgress<TableRebuildProgress>
        {
            private readonly Control control;
            private readonly Action<TableRebuildProgress> callback;
            private readonly object gate = new object();
            private TableRebuildProgress pending;
            private bool updateQueued;

            public CoalescingProgress(Control control, Action<TableRebuildProgress> callback)
            {
                this.control = control;
                this.callback = callback;
            }

            public void Report(TableRebuildProgress value)
            {
                lock (gate)
                {
                    pending = value;
                    if (updateQueued) return;
                    updateQueued = true;
                }

                try
                {
                    control.BeginInvoke((Action)Flush);
                }
                catch (ObjectDisposedException)
                {
                    // Form/handle already gone; nothing left to update.
                }
                catch (InvalidOperationException)
                {
                    // No window handle yet (shouldn't happen post-OnShown) or already disposed.
                }
            }

            private void Flush()
            {
                TableRebuildProgress value;
                lock (gate)
                {
                    value = pending;
                    updateQueued = false;
                }

                callback(value);
            }
        }
    }
}
