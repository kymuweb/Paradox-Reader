using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

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
    }
}
