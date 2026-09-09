using System;
using System.IO;
using System.Threading;

namespace ParadoxReader
{
    /// <summary>
    /// Manages file-level locking for a Paradox table to allow safe
    /// parallel operation with BDE (Borland Database Engine).
    ///
    /// Uses a .LCK file alongside the .DB file (same convention as BDE)
    /// and an in-process Mutex to protect against concurrent threads.
    /// </summary>
    public class ParadoxFileLock : IDisposable
    {
        // ----------------------------------------------------------------
        // Constants
        // ----------------------------------------------------------------

        private const int LOCK_TIMEOUT_MS       = 5000;
        private const int LOCK_RETRY_INTERVAL_MS = 100;

        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        private readonly string     lckFilePath;
        private readonly string     mutexName;
        private          FileStream lckStream;
        private          Mutex      mutex;
        private          bool       isLocked;
        private          int        lockDepth;

        // ----------------------------------------------------------------
        // Constructor
        // ----------------------------------------------------------------

        public ParadoxFileLock(string dbFilePath)
        {
            lckFilePath = Path.ChangeExtension(dbFilePath, ".LCK");
            mutexName   = "ParadoxLock_" +
                          dbFilePath.ToUpperInvariant()
                                    .Replace('\\', '_')
                                    .Replace(':',  '_')
                                    .Replace('/',  '_');
        }

        // ----------------------------------------------------------------
        // Acquire / Release
        // ----------------------------------------------------------------

        /// <summary>
        /// Acquires an exclusive write lock. Reentrant: nested
        /// Acquire/Release pairs (e.g. an outer batch scope wrapping many
        /// individual per-record operations, as used by
        /// <see cref="ParadoxTableFile.AcquireScopedBatchWriteLock"/>) only
        /// take the underlying mutex/.LCK file once, on the outermost call;
        /// the .LCK file is only written/deleted once for the whole batch
        /// instead of once per nested call, avoiding redundant disk I/O.
        /// Blocks until the lock is available or the timeout expires.
        /// </summary>
        public void AcquireWriteLock()
        {
            if (isLocked)
            {
                lockDepth++;
                return;
            }

            // 1. In-process mutex
            mutex = new Mutex(false, mutexName);
            if (!mutex.WaitOne(LOCK_TIMEOUT_MS))
            {
                mutex.Close();
                mutex = null;
                throw new TimeoutException(
                    $"Could not acquire in-process lock for '{lckFilePath}' " +
                    $"within {LOCK_TIMEOUT_MS}ms.");
            }

            // 2. OS-level .LCK file lock — retry until timeout
            int elapsed = 0;
            while (true)
            {
                try
                {
                    lckStream = new FileStream(
                        lckFilePath,
                        FileMode.OpenOrCreate,
                        FileAccess.ReadWrite,
                        FileShare.None); // Exclusive

                    lckStream.SetLength(0);
                    lckStream.WriteByte(0x01); // Write-lock sentinel
                    lckStream.Flush();
                    isLocked  = true;
                    lockDepth = 1;
                    return;
                }
                catch (IOException)
                {
                    if (elapsed >= LOCK_TIMEOUT_MS)
                    {
                        ReleaseMutex();
                        throw new TimeoutException(
                            $"Could not acquire file lock for '{lckFilePath}' " +
                            $"within {LOCK_TIMEOUT_MS}ms. BDE may hold the lock.");
                    }
                    Thread.Sleep(LOCK_RETRY_INTERVAL_MS);
                    elapsed += LOCK_RETRY_INTERVAL_MS;
                }
            }
        }

        /// <summary>
        /// Releases the write lock, allowing BDE and other processes to
        /// proceed. Reentrant: when the lock was acquired via nested calls
        /// to <see cref="AcquireWriteLock"/>, this only decrements the
        /// nesting depth until the outermost call releases it, so the
        /// .LCK file is only deleted once the outermost scope completes.
        /// </summary>
        public void ReleaseWriteLock()
        {
            if (!isLocked) return;

            if (lockDepth > 1)
            {
                lockDepth--;
                return;
            }

            try
            {
                lckStream?.SetLength(0);
                lckStream?.Flush();
                lckStream?.Dispose();
                lckStream = null;

                try { if (File.Exists(lckFilePath)) File.Delete(lckFilePath); }
                catch { /* BDE may recreate it — ignore */ }
            }
            finally
            {
                ReleaseMutex();
                isLocked  = false;
                lockDepth = 0;
            }
        }

        // ----------------------------------------------------------------
        // Scoped lock (use with `using`)
        // ----------------------------------------------------------------

        /// <summary>
        /// Returns a scoped write lock that is automatically released on Dispose.
        /// Use with a <c>using</c> statement.
        /// </summary>
        public ScopedWriteLock AcquireScopedWriteLock()
        {
            AcquireWriteLock();
            return new ScopedWriteLock(this);
        }

        public sealed class ScopedWriteLock : IDisposable
        {
            private readonly ParadoxFileLock owner;
            internal ScopedWriteLock(ParadoxFileLock owner) { this.owner = owner; }
            public void Dispose() => owner.ReleaseWriteLock();
        }

        // ----------------------------------------------------------------
        // Helpers
        // ----------------------------------------------------------------

        private void ReleaseMutex()
        {
            if (mutex == null) return;
            try   { mutex.ReleaseMutex(); }
            catch { /* Already released */ }
            mutex.Close();
            mutex = null;
        }

        // ----------------------------------------------------------------
        // IDisposable
        // ----------------------------------------------------------------

        public void Dispose() => ReleaseWriteLock();
    }
}