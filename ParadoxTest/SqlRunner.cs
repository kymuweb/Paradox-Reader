using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace ParadoxTest
{
    /// <summary>
    /// Consolidated helper for invoking Medilink.Tools.SQLRunner.exe against a
    /// Paradox .DB table for test/diagnostic purposes, e.g.:
    ///
    ///   Medilink.Tools.SQLRunner.exe /S "select count(*) from 'c:\temp\testtab.db'"
    ///   Medilink.Tools.SQLRunner.exe /S "delete from 'c:\temp\testtab.db'"
    ///
    /// (/S = silent). Runs are always synchronous (no async/await) and
    /// single-instance: every call first force-kills any stray SQLRunner
    /// process left over from a previous (possibly hung) invocation and
    /// clears any leftover *.LCK files (including PDOXUSRS.LCK), since
    /// running multiple SQLRunner/BDE instances concurrently against the
    /// same table is itself a reliable way to cause locking problems.
    /// </summary>
    internal static class SqlRunner
    {
        /// <summary>
        /// Machine-specific path to Medilink.Tools.SQLRunner.exe, sourced from
        /// app.config's appSettings (via SqlRunner.local.config, git-ignored -
        /// see SqlRunner.local.config.example). Empty when unset/missing.
        /// </summary>
        public static string ExePath => Configuration.GetSqlRunnerExePath();

        public static bool IsAvailable => !string.IsNullOrEmpty(ExePath) && File.Exists(ExePath);

        public sealed class Result
        {
            public bool Exited;
            public string Stdout = string.Empty;
            public string Stderr = string.Empty;
        }

        /// <summary>
        /// Confirms functional readability AND correctness via SQLRunner's
        /// "select count(*) from '&lt;dbPath&gt;'" oracle - a real BDE engine
        /// opening/scanning the table end to end, which is a much stronger
        /// signal that the on-disk file is genuinely well-formed than "this
        /// library can still read the bytes back". A healthy table produces
        /// exactly one "Read 1 rows." line (always 1, regardless of the
        /// table's actual record count - even an empty table is still "1
        /// result") followed by a "Count: N" line where N is the real record
        /// count; anything else (including SQLRunner falling back to
        /// dumping one line per record, or producing no Read/Count lines at
        /// all) indicates SQLRunner/BDE considers the table broken.
        /// </summary>
        /// <param name="dbPath">Full path to the .DB table to query.</param>
        /// <param name="label">Short label used in diagnostic console output.</param>
        /// <param name="expectedRecordCount">The record count the table is expected to report.</param>
        /// <param name="preStdinDelayMs">
        /// Delay before feeding the dismiss-prompt ENTER keystroke to stdin.
        /// A full-table "select count(*)" can take noticeably longer than a
        /// trivial operation over a blob-bearing table, so give SQLRunner
        /// extra time by default to compute/print its result first.
        /// </param>
        /// <param name="timeoutMs">
        /// How long to wait for SQLRunner to exit before treating it as hung.
        /// </param>
        public static bool CountOracle(string dbPath, string label, int expectedRecordCount, int preStdinDelayMs = 15000, int timeoutMs = 30000)
        {
            if (!File.Exists(dbPath))
            {
                Console.WriteLine("  [{0}] SKIP (file not found: {1})", label, dbPath);
                return false;
            }

            var result = Execute("select count(*) from '" + dbPath + "'", lockFileFolder: Path.GetDirectoryName(dbPath), preStdinDelayMs: preStdinDelayMs, timeoutMs: timeoutMs);
            string stdout = result.Stdout;
            string stderr = result.Stderr;

            var readMatch = Regex.Match(stdout, @"Read (\d+) rows?\.", RegexOptions.IgnoreCase);
            var countMatch = Regex.Match(stdout, @"Count:\s*(-?\d+)", RegexOptions.IgnoreCase);

            if (!result.Exited || !readMatch.Success || !countMatch.Success)
            {
                Console.WriteLine("  [{0}] FAIL (could not parse \"Read N rows.\" / \"Count: N\" lines from SQLRunner output)", label);
                Console.WriteLine("  ---- raw stdout ----");
                Console.WriteLine(stdout);
                Console.WriteLine("  ---- raw stderr ----");
                Console.WriteLine(stderr);
                Console.WriteLine("  ---------------------");
                return false;
            }

            int readRows = int.Parse(readMatch.Groups[1].Value);
            int actualCount = int.Parse(countMatch.Groups[1].Value);

            bool readOk = readRows == 1; // count(*) must always be a single aggregate result
            bool countOk = actualCount == expectedRecordCount;
            bool ok = readOk && countOk;

            Console.WriteLine("  [{0}] {1} (read {2} row(s) [expected 1], Count={3} [expected {4}])",
                label, ok ? "PASS" : "FAIL", readRows, actualCount, expectedRecordCount);
            return ok;
        }

        /// <summary>
        /// Runs one SQLRunner statement (SELECT/INSERT/UPDATE/DELETE/CREATE)
        /// with the /S (silent) flag, synchronously.
        /// </summary>
        /// <param name="sql">
        /// The SQL statement, e.g. "select count(*) from 'c:\temp\testtab.db'".
        /// For "select count(*) from ...", success is exactly one "Read 1
        /// rows." line plus a "Count: N" line; more than one "Read" row means
        /// SQLRunner fell back to dumping one line per record (failure mode).
        /// </param>
        /// <param name="lockFileFolder">
        /// Folder containing the table being operated on, used to check/clear
        /// *.LCK files (including PDOXUSRS.LCK) before and after the call.
        /// Pass null to skip lock-file handling.
        /// </param>
        /// <param name="preStdinDelayMs">
        /// Delay before feeding the dismiss-prompt ENTER keystroke to stdin.
        /// SQLRunner needs time to compute/print its result first for slower
        /// operations (e.g. a full-table "select count(*)" over a
        /// blob-bearing table); 0 (the default) feeds it immediately, which
        /// is fine for fast operations (INSERT/UPDATE/DELETE/simple SELECT).
        /// </param>
        /// <param name="timeoutMs">
        /// How long to wait for SQLRunner to exit before treating it as hung
        /// and force-killing it (and any child processes).
        /// </param>
        public static Result Execute(string sql, string lockFileFolder = null, int preStdinDelayMs = 0, int timeoutMs = 10000)
        {
            Console.WriteLine("SQLRunner> {0}", sql);

            // Guarantee a clean starting state before every single invocation:
            // kill any lingering SQLRunner process (a prior call may have
            // hung and been force-killed, or a stray instance may still be
            // shutting down) and remove all *.LCK files, then verify both are
            // actually gone before we start a new process. Without this,
            // multiple SQLRunner instances can pile up concurrently and
            // fight over the same locks, which is its own source of hangs.
            EnsureNoStrayProcesses();
            if (lockFileFolder != null)
            {
                DeleteLockFiles(lockFileFolder);

                var remainingLocks = Directory.GetFiles(lockFileFolder, "*.LCK");
                if (remainingLocks.Length > 0)
                {
                    Console.WriteLine("  [warn] {0} lock file(s) still present before launch: {1}",
                        remainingLocks.Length, string.Join(", ", remainingLocks.Select(Path.GetFileName)));
                }
            }

            var psi = new ProcessStartInfo
            {
                FileName               = ExePath,
                Arguments              = $"/S \"{sql}\"",
                UseShellExecute        = false,
                RedirectStandardInput  = true,
                RedirectStandardOutput = true,
                RedirectStandardError  = true,
                CreateNoWindow         = true
            };

            var stdoutBuilder = new StringBuilder();
            var stderrBuilder = new StringBuilder();
            var result = new Result();

            using (var process = new Process { StartInfo = psi, EnableRaisingEvents = true })
            {
                process.OutputDataReceived += (s, e) => { if (e.Data != null) stdoutBuilder.AppendLine(e.Data); };
                process.ErrorDataReceived  += (s, e) => { if (e.Data != null) stderrBuilder.AppendLine(e.Data); };

                process.Start();
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();

                if (preStdinDelayMs > 0)
                    System.Threading.Thread.Sleep(preStdinDelayMs);

                // SQLRunner's /S flag appears to suppress the confirmation
                // prompt only for INSERT; UPDATE/DELETE still print "Please
                // ensure you have a Backup." and then wait on stdin for an
                // ENTER keypress before proceeding, which our redirected
                // (non-interactive) stdin never provides. Feed a single
                // newline proactively so it doesn't block waiting for input
                // that will never come.
                try
                {
                    process.StandardInput.WriteLine();
                    process.StandardInput.Flush();
                }
                catch { /* process may have already exited or not be waiting on stdin */ }

                result.Exited = process.WaitForExit(timeoutMs);
                if (!result.Exited)
                {
                    Console.WriteLine("  [warn] SQLRunner did not exit within {0}ms; treating as HUNG. Killing process.", timeoutMs);
                    KillProcessTree(process);
                    process.WaitForExit();
                }
            }

            // BDE can hold onto its file handles / PDOXUSRS.LCK briefly after
            // the SQLRunner process itself has exited (e.g. while its BDE
            // session/engine shuts down). Give it a moment before we attempt
            // to touch the table or its lock files again, otherwise a
            // following ParadoxTableFile open (or another SQLRunner call)
            // can race BDE's own cleanup and see a "table is busy" state.
            System.Threading.Thread.Sleep(lockFileFolder != null ? 500 : 300);

            result.Stdout = stdoutBuilder.ToString();
            result.Stderr = stderrBuilder.ToString();

            if (!string.IsNullOrWhiteSpace(result.Stdout))
                Console.WriteLine(result.Stdout.Trim());
            if (!string.IsNullOrWhiteSpace(result.Stderr))
                Console.WriteLine("  [stderr] " + result.Stderr.Trim());

            // Clean up any lock file SQLRunner itself left behind, so the
            // next invocation (or a subsequent ParadoxTableFile open) doesn't
            // see a stale lock. Retries with backoff since BDE may still be
            // releasing the handle for a short time after the process exits.
            if (lockFileFolder != null)
                DeleteLockFiles(lockFileFolder);

            return result;
        }

        /// <summary>
        /// Guarantees no SQLRunner process is left running before we launch a
        /// new one. A previous invocation that hung and was force-killed can,
        /// in rare cases, leave a still-shutting-down instance behind (or a
        /// completely separate stray instance from an earlier crashed run of
        /// this harness). Running multiple SQLRunner/BDE instances
        /// concurrently against the same table is itself a reliable way to
        /// cause locking problems, so this is checked/cleared before every
        /// single Execute call.
        /// </summary>
        public static void EnsureNoStrayProcesses()
        {
            var exeName = Path.GetFileNameWithoutExtension(ExePath);
            if (string.IsNullOrEmpty(exeName)) return;

            var stray = Process.GetProcessesByName(exeName);
            if (stray.Length == 0) return;

            Console.WriteLine("  [warn] {0} stray SQLRunner process(es) found before launch (PIDs: {1}); killing.",
                stray.Length, string.Join(", ", stray.Select(p => p.Id.ToString())));

            foreach (var p in stray)
            {
                try { KillProcessTree(p); }
                catch (Exception ex) { Console.WriteLine("  [warn] Failed to kill PID {0}: {1}", p.Id, ex.Message); }
                finally { p.Dispose(); }
            }

            // Give the OS a moment to fully tear down the killed process(es)
            // before we go on to check/delete lock files.
            System.Threading.Thread.Sleep(500);

            var stillRunning = Process.GetProcessesByName(exeName);
            if (stillRunning.Length > 0)
            {
                Console.WriteLine("  [warn] {0} SQLRunner process(es) still running after kill attempt (PIDs: {1}).",
                    stillRunning.Length, string.Join(", ", stillRunning.Select(p => p.Id.ToString())));
                foreach (var p in stillRunning) p.Dispose();
            }
        }

        /// <summary>
        /// Deletes any *.LCK files (including PDOXUSRS.LCK) in
        /// <paramref name="folder"/>, retrying briefly if a file is still in
        /// use (BDE can hold the handle open for a short time after
        /// SQLRunner's process has exited).
        /// </summary>
        public static void DeleteLockFiles(string folder, int maxAttempts = 5, int delayMs = 250)
        {
            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                var remaining = Directory.GetFiles(folder, "*.LCK");
                if (remaining.Length == 0) return;

                bool anyFailed = false;
                foreach (var lockFile in remaining)
                {
                    try { File.Delete(lockFile); }
                    catch (Exception ex)
                    {
                        anyFailed = true;
                        if (attempt == maxAttempts)
                            Console.WriteLine("  [warn] Could not delete {0} after {1} attempts: {2}", lockFile, maxAttempts, ex.Message);
                    }
                }

                if (!anyFailed) return;
                System.Threading.Thread.Sleep(delayMs);
            }
        }

        /// <summary>
        /// Kills a process and any child processes it may have spawned
        /// (taskkill /T), rather than relying on Process.Kill() alone, which
        /// only kills the immediate process and can leave children running.
        /// </summary>
        private static void KillProcessTree(Process process)
        {
            try
            {
                using (var killer = new Process())
                {
                    killer.StartInfo = new ProcessStartInfo
                    {
                        FileName        = "taskkill",
                        Arguments       = $"/PID {process.Id} /T /F",
                        UseShellExecute = false,
                        CreateNoWindow  = true,
                        RedirectStandardOutput = true,
                        RedirectStandardError  = true
                    };
                    killer.Start();
                    killer.WaitForExit(5000);
                }
            }
            catch { /* fall back to direct kill below */ }

            try { if (!process.HasExited) process.Kill(); } catch { /* best effort */ }
        }
    }
}
