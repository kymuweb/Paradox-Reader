using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace ParadoxTest
{
    internal static class Configuration
    {
        private static string GetConnectionString(string name)
        {
            try
            {
                return System.Configuration.ConfigurationManager.ConnectionStrings[name]?.ConnectionString ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        internal static string GetParadoxDataFolderPath(string connectionStringName)
        {

            var connectionString_DataSource = string.Empty;
            var connectionString_InitialCatalog = string.Empty;

            var connectionString = GetConnectionString(connectionStringName);
            if (!string.IsNullOrEmpty(connectionString))
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                connectionString_DataSource = builder.DataSource;
                connectionString_InitialCatalog = builder.InitialCatalog;
            }

            var dbPath = string.Empty;
            if (!string.IsNullOrEmpty(connectionString_DataSource))
            {
                dbPath = (new DirectoryInfo(connectionString_DataSource))?.FullName ?? connectionString_DataSource;
            }
            if (string.IsNullOrEmpty(dbPath))
            {
                dbPath = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            }
            if (!string.IsNullOrEmpty(connectionString_InitialCatalog))
            {
                dbPath = Path.Combine(dbPath, connectionString_InitialCatalog);
            }
            return dbPath;
        }

        // Machine-specific; deliberately NOT hard-coded so it never needs to
        // be committed. Configured via appSettings key "SqlRunnerExePath",
        // whose value normally comes from ParadoxTest\SqlRunner.local.config
        // (git-ignored - see SqlRunner.local.config.example for the template).
        // Falls back to string.Empty when unset/missing, which callers treat
        // the same as "SQLRunner not found" and skip gracefully.
        internal static string GetSqlRunnerExePath()
        {
            try
            {
                return System.Configuration.ConfigurationManager.AppSettings["SqlRunnerExePath"] ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        // Machine-specific real-world corpus directory used by corpustest /
        // xg0diag / xg0dumpmany default paths. Configured via appSettings key
        // "CorpusDataRootPath", normally set in ParadoxTest\SqlRunner.local.config
        // (git-ignored - see SqlRunner.local.config.example). Falls back to
        // string.Empty when unset/missing.
        internal static string GetCorpusDataRootPath()
        {
            try
            {
                return System.Configuration.ConfigurationManager.AppSettings["CorpusDataRootPath"] ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        // Machine-specific; WriteRows.exe. Read from the appSettings-style
        // file LegacyWriteRows.local.config (git-ignored - see
        // LegacyWriteRows.local.config.example), key "LegacyWriteRowsExeFilePath". It is loaded
        // directly because app.config's appSettings can only reference one external file       
        // (already used by SqlRunner.local.config). Falls back to string.Empty.
        internal static string GetLegacyWriteRowsExeFilePath()
        {
            return GetLegacyWriteRowsSetting("LegacyWriteRowsExeFilePath");
        }

        // Machine-specific folder containing the Paradox tables the legacy
        // WriteRows.exe writes to. Same source/logic as GetLegacyWriteRowsExeFilePath,
        // key "DatabaseFolderPath". Falls back to string.Empty.
        internal static string GetLegacyWriteRowsDatabaseFolderPath()
        {
            return GetLegacyWriteRowsSetting("LegacyWriteRowsDatabaseFolderPath");
        }

        private static string GetLegacyWriteRowsSetting(string key)
        {
            try
            {
                var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "LegacyWriteRows.local.config");
                if (!File.Exists(path)) return string.Empty;

                var doc = System.Xml.Linq.XDocument.Load(path);
                return doc.Descendants("add")
                    .Where(e => string.Equals((string)e.Attribute("key"), key, StringComparison.OrdinalIgnoreCase))
                    .Select(e => (string)e.Attribute("value"))
                    .FirstOrDefault() ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        // Machine-specific; the BDE "Pdxrbld"
        // used as the known-good reference implementation to compare
        // ParadoxReader.TableRebuilder's output against. Configured via
        // appSettings key "PdxrbldExePath", normally set in
        // ParadoxTest\SqlRunner.local.config (git-ignored - see
        // SqlRunner.local.config.example). Falls back to string.Empty when
        // unset/missing.
        internal static string GetPdxrbldExePath()
        {
            try
            {
                return System.Configuration.ConfigurationManager.AppSettings["PdxrbldExePath"] ?? string.Empty;
            }
            catch
            {
                return string.Empty;
            }
        }

        // Passwords passed to Pdxrbld (one /P argument each) so password-protected
        // tables never block on its "Enter master password" prompt. Configured via
        // appSettings key "PdxrbldPasswords" (semicolon-separated) in the git-ignored
        // SqlRunner.local.config. Defaults to the example password "cfjxcut".
        internal static string[] GetPdxrbldPasswords()
        {
            string value = null;
            try
            {
                value = System.Configuration.ConfigurationManager.AppSettings["PdxrbldPasswords"];
            }
            catch
            {
            }
            if (string.IsNullOrWhiteSpace(value)) return new[] { "cfjxcut" };
            return value.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(p => p.Trim())
                .Where(p => p.Length > 0)
                .ToArray();
        }
    }
}
