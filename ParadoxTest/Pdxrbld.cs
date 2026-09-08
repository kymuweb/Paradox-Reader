using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace ParadoxTest
{
    /// <summary>
    /// Consolidated helper for invoking BDE's Pdxrbld.exe (Paradox table
    /// rebuild utility), used as a known-good reference implementation to
    /// verify/compare against ParadoxReader.TableRebuilder's own output, e.g.:
    ///
    ///   Pdxrbld.exe /Fc:\temp /Ttesttab.db -R2 -L+ -P+ -Q+
    ///
    ///   /F&lt;datafolderpath&gt;  data folder (no '=' between flag and value)
    ///   /T&lt;tablefilename&gt;   table file name (no '=' between flag and value)
    ///   -R2                    verify + rebuild (so the log records whether
    ///                          the table did/didn't already need rebuilding)
    ///   -L+                    logging
    ///   -P+/-P-                pack (or don't pack) the table
    ///   -Q+                    quiet/silent mode
    ///
    /// Runs are always synchronous (no async/await) and single-instance: only
    /// one Pdxrbld invocation is ever allowed to run at a time, so a stray
    /// instance from a previous (possibly hung) call is force-killed before
    /// launching a new one.
    /// </summary>
    internal static class Pdxrbld
    {
        /// <summary>
        /// Machine-specific path to Pdxrbld.exe, sourced from app.config's
        /// appSettings (via SqlRunner.local.config, git-ignored - see
        /// SqlRunner.local.config.example). Empty when unset/missing.
        /// </summary>
        public static string ExePath => Configuration.GetPdxrbldExePath();

        public static bool IsAvailable => !string.IsNullOrEmpty(ExePath) && File.Exists(ExePath);

        /// <summary>
        /// Runs Pdxrbld against <paramref name="tableFileName"/> in
        /// <paramref name="dataFolder"/>, always doing a verify + rebuild
        /// (-R2), with logging (-L+) and quiet/silent mode (-Q+). Confirms
        /// success by reading the tail of Pdxrbld's own append-only log file
        /// (Pdxrbld.LOG, next to Pdxrbld.exe) for a "no errors found" entry
        /// for this table, appended after this invocation started.
        /// </summary>
        /// <param name="dataFolder">Folder containing the .DB file (Pdxrbld's /F argument).</param>
        /// <param name="tableFileName">Table file name, e.g. "testtab.db" (Pdxrbld's /T argument).</param>
        /// <param name="pack">Whether to pack the table (-P+); pass false to skip packing (-P-).</param>
        /// <param name="timeoutMs">How long to wait before treating Pdxrbld as hung and killing it.</param>
        public static bool Rebuild(string dataFolder, string tableFileName, bool pack = true, int timeoutMs = 15000)
        {
            EnsureNoStrayProcesses();

            string logPath = Path.Combine(Path.GetDirectoryName(ExePath) ?? string.Empty, "Pdxrbld.LOG");
            long logLengthBefore = File.Exists(logPath) ? new FileInfo(logPath).Length : 0;

            var psi = new ProcessStartInfo
            {
                FileName = ExePath,
                Arguments = string.Format("\"/F{0}\" \"/T{1}\" -R2 {2} -L+ -Q+", dataFolder, tableFileName, pack ? "-P+" : "-P-"),
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            Console.WriteLine("Pdxrbld> {0}", psi.Arguments);

            using (var process = new Process { StartInfo = psi })
            {
                process.Start();
                if (!process.WaitForExit(timeoutMs))
                {
                    Console.WriteLine("  [warn] Pdxrbld did not exit within {0}ms; treating as HUNG. Killing process.", timeoutMs);
                    try { process.Kill(); } catch { /* best effort */ }
                    process.WaitForExit();
                    return false;
                }
            }

            if (!File.Exists(logPath))
            {
                Console.WriteLine("  [warn] Pdxrbld.LOG not found at {0}; cannot confirm result.", logPath);
                return false;
            }

            string newLogText;
            using (var fs = new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                fs.Position = Math.Min(logLengthBefore, fs.Length);
                using (var r = new StreamReader(fs))
                    newLogText = r.ReadToEnd();
            }

            Console.WriteLine(newLogText.Trim());

            string baseNameNoExt = Path.GetFileNameWithoutExtension(tableFileName).ToUpperInvariant();
            bool noErrors = newLogText.ToUpperInvariant().Contains(baseNameNoExt + ".DB") &&
                             newLogText.ToUpperInvariant().Contains("NO ERRORS FOUND");

            if (!noErrors)
                Console.WriteLine("  [FAIL] Pdxrbld log did not report \"no errors found\" for {0}.", tableFileName);

            return noErrors;
        }

        /// <summary>
        /// Guarantees no other Pdxrbld process is left running before we
        /// launch a new one; only one instance should ever run at a time.
        /// </summary>
        private static void EnsureNoStrayProcesses()
        {
            var exeName = Path.GetFileNameWithoutExtension(ExePath);
            if (string.IsNullOrEmpty(exeName)) return;

            var stray = Process.GetProcessesByName(exeName);
            if (stray.Length == 0) return;

            Console.WriteLine("  [warn] {0} stray Pdxrbld process(es) found before launch (PIDs: {1}); killing.",
                stray.Length, string.Join(", ", stray.Select(p => p.Id.ToString())));

            foreach (var p in stray)
            {
                try
                {
                    if (!p.HasExited) p.Kill();
                    p.WaitForExit(5000);
                }
                catch { /* best effort */ }
                finally { p.Dispose(); }
            }

            System.Threading.Thread.Sleep(500);
        }
    }
}
