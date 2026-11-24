using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using static ParadoxTest.LegacyWriteRows;

namespace ParadoxTest
{
    /// <summary>
    /// Wrapper around the legacy WriteRows.exe tool.
    /// NOTE: this is for KNOWN STRUCTURES ONLY. WriteRows.exe can only write to the pre-existing tables
    /// (with their existing schemas) in the legacy database folder (LegacyWriteRowsDatabaseFolderPath,
    /// e.g. C:\Temp\data); it cannot
    /// Tests using it must therefore operate on a real table in that folder (back it up first and restore it afterwards).
    /// </summary>
    public static class LegacyWriteRows
    {

        /// <summary>
        /// Path to WriteRows.exe. Machine-specific, read from the
        /// git-ignored LegacyWriteRows.local.config (see LegacyWriteRows.local.config.example).
        /// Empty when unset/missing.
        /// </summary>
        public static string LegacyWriteRowsExeFilePath => Configuration.GetLegacyWriteRowsExeFilePath();
        public static string LegacyWriteRowsDatabaseFolderPath => Configuration.GetLegacyWriteRowsDatabaseFolderPath();

        /// <summary>
        /// This is what we feed in to the tool via json file. It can be an array of these values.
        /// </summary>
        public class WriteRowInput
        {
            public Guid RowId { get; set; } // Example: "rowId": "aff3befe-5ff0-48ea-b040-aa178b35ffaa"  - This is our internal unique identifier for the row to keep track of our writes. It is *not* the database identifier for the row.
            public string TableName { get; set; } // Example: "tableName": "clientversionlog.db"  - it doesn't really have to be the full path.
            public Dictionary<string, object> ColumnValues { get; set; } /* Example: "columnValues": {
      "Id": 0,
      "MajorVersion": 1,
      "MinorVersion": 0,
      "VersionName": "1.0.0",
      "VersionBuildDate": "2023-06-01T12:00:00Z",
      "ComputerName": "MyComputer",
      "UserName": "MyUser",
      "ServerBaseDir": "C:\\Server\\Base\\Dir",
      "ClientId": 12345,
      "OperatingSystem": "Windows 10",
      "DateTime": "2024-06-01T12:00:00Z"
            }
             - Note that autoinc values (some ids) should be zero but they will return a different value in the output.
            */
    }
        

    /// <summary>
    /// This is what we get back from the tool from the output json file. It is a super set of the input however in some cases the column values may be different (i.e. if autoinc field it may now be set). It can be an array of these values.
    /// </summary>
        public class WriteRowOutput : WriteRowInput
    {
            public string Status { get; set; } // Example: "status": "Success" or "Failure"
            public string ErrorMessage { get; set; } // Example: "errorMessage": "Some error message if status is Failure"
            public DateTime WriteTimestampUtc { get; set; } // Example: "writeTimestampUtc": "2024-06-01T12:00:00Z"  - This is the timestamp when the write operation was performed.}

            
        }

        public static bool WriteRow(string tableName, Dictionary<string, object> columnValues)
        {
            return WriteRows(new List<(string TableName, Dictionary<string, object> ColumnValues)> { (tableName, columnValues) });
        }

        public static bool WriteRows(IEnumerable<(string tableName, Dictionary<string, object> columnValues)> tableNamesAndColumnValues)
        {
            var inputJsonFilePath = Path.Combine(Path.GetTempPath(), "writeRowInput.json");
            var outputJsonFilePath = Path.Combine(Path.GetTempPath(), "writeRowOutput.json");

            File.Delete(outputJsonFilePath); // Delete the output file if it exists, so we can check for its existence later.

            var inputs = new List<WriteRowInput>();

            foreach (var (tableName, columnValues) in tableNamesAndColumnValues)
            {
                var input = new WriteRowInput
                {
                    RowId = Guid.NewGuid(),
                    TableName = tableName,
                    ColumnValues = columnValues
                };
                inputs.Add(input);
            }

            var inputJson = System.Text.Json.JsonSerializer.Serialize(inputs);

            // The legacy tool must never run in parallel (or back-to-back too quickly): wait for any
            // running instance, pause between calls, and retry when it fails to start / emits no output.
            lock (WriteRowsLock)
            {
                string lastError = null;
                string lastOutputJson = null;
                for (int attempt = 1; attempt <= MaxAttempts; attempt++)
                {
                    WaitForNoWriteRowsProcess();
                    File.Delete(outputJsonFilePath);
                    File.WriteAllText(inputJsonFilePath, inputJson);
                    bool timedOut = false;
                    // Call the external tool to write the row
                    var processInfo = new System.Diagnostics.ProcessStartInfo
                    {
                        FileName = LegacyWriteRowsExeFilePath,
                        Arguments = $"-i \"{inputJsonFilePath}\" -o \"{outputJsonFilePath}\" -s", // -s = silent so it doesn't wait for input
                        RedirectStandardOutput = false,
                        UseShellExecute = false,
                        CreateNoWindow = true
                    };
                    using (var process = System.Diagnostics.Process.Start(processInfo))
                    {
                        if (!process.WaitForExit(ProcessTimeoutMs))
                        {
                            timedOut = true;
                            try { process.Kill(); } catch { }
                        }
                    }
                    System.Threading.Thread.Sleep(InterWriteDelayMs);

                    if (!File.Exists(outputJsonFilePath))
                    {
                        lastError = timedOut
                            ? $"Output file was not created by the external tool (timed out after {ProcessTimeoutMs / 1000}s)."
                            : "Output file was not created by the external tool.";
                        continue;
                    }

                    lastOutputJson = File.ReadAllText(outputJsonFilePath);
                    List<WriteRowOutput> output;
                    try
                    {
                        output = System.Text.Json.JsonSerializer.Deserialize<List<WriteRowOutput>>(
                            lastOutputJson,
                            new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                    }
                    catch (Exception ex)
                    {
                        lastError = "Could not parse tool output: " + ex.Message;
                        continue;
                    }

                    var failed = output.FirstOrDefault(i => i.Status?.Equals("Success", StringComparison.OrdinalIgnoreCase) != true);
                    if (failed == null)
                    {
                        // TODO: consider outputing the output to a log file or returning it to the caller for further processing if needed. We would need to keep the values in the same order as the input so that the caller can match them up. For now we just return true if all writes were successful.
                        return true;
                    }
                    lastError = $"Failed to write row: {failed.ErrorMessage}";
                }

                LogFailure(inputs, inputJson, lastError, lastOutputJson);
                throw new Exception(lastError + $" (failed {MaxAttempts} attempts; input logged to {FailedInputsJsonFilePath})");
            }
        }

        private static readonly object WriteRowsLock = new object();

        /// <summary>Total attempts (first try + retries) before a write is treated as failed.</summary>
        public static int MaxAttempts = 3;

        /// <summary>The tool normally finishes within ~1s; kill it if it is still running after this.</summary>
        public static int ProcessTimeoutMs = 10000;

        /// <summary>Pause after every legacy tool run so consecutive runs never overlap.</summary>
        public static int InterWriteDelayMs = 1000;

        /// <summary>Accumulated inputs of writes that failed all attempts. A JSON array that can be fed straight back to the tool with -i.</summary>
        public static string FailedInputsJsonFilePath = @"C:\temp\LegacyWriteRows_failed_inputs.json";

        /// <summary>Human-readable details (timestamp, error, tool output) for the entries in <see cref="FailedInputsJsonFilePath"/>.</summary>
        public static string FailedDetailsLogFilePath = @"C:\temp\LegacyWriteRows_failed_details.log";

        private static void LogFailure(List<WriteRowInput> inputs, string inputJson, string error, string outputJson)
        {
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(FailedInputsJsonFilePath));

                var all = new List<WriteRowInput>();
                if (File.Exists(FailedInputsJsonFilePath))
                {
                    var existing = System.Text.Json.JsonSerializer.Deserialize<List<WriteRowInput>>(File.ReadAllText(FailedInputsJsonFilePath));
                    if (existing != null) all.AddRange(existing);
                }
                all.AddRange(inputs);
                File.WriteAllText(FailedInputsJsonFilePath,
                    System.Text.Json.JsonSerializer.Serialize(all, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));

                File.AppendAllText(FailedDetailsLogFilePath,
                    $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {error}{Environment.NewLine}  input:  {inputJson}{Environment.NewLine}  output: {outputJson ?? "<none>"}{Environment.NewLine}{Environment.NewLine}");
            }
            catch
            {
                // Logging must never mask the original failure.
            }
        }

        private static void WaitForNoWriteRowsProcess()
        {
            var name = Path.GetFileNameWithoutExtension(LegacyWriteRowsExeFilePath);
            var deadline = DateTime.UtcNow.AddSeconds(90);
            while (DateTime.UtcNow < deadline)
            {
                var running = System.Diagnostics.Process.GetProcessesByName(name);
                bool any = running.Length > 0;
                foreach (var p in running) p.Dispose();
                if (!any) return;
                System.Threading.Thread.Sleep(500);
            }
        }

    }
}
