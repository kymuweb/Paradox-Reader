using System;
using System.Collections.Generic;
using System.Data.SqlClient;
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
    }
}
