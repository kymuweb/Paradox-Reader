using System;
using System.Data;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Minimal <see cref="IDbConnection"/> wrapper over <see cref="ParadoxSqlExecutor"/>,
    /// allowing standard ADO.NET-style code (DataAdapter, generic data-bound grids,
    /// reporting tools) to query/modify Paradox tables via SQL text, without any
    /// bespoke Paradox-aware UI. The "connection string" is simply the base
    /// directory used to resolve bare table names in SQL statements.
    /// </summary>
    public sealed class ParadoxConnection : IDbConnection
    {
        internal ParadoxSqlExecutor Executor { get; private set; }

        public ParadoxConnection() { }

        public ParadoxConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// The base directory used to resolve bare table names (e.g. "testtab")
        /// referenced in SQL statements. Full/relative paths in SQL text are
        /// still honored as-is.
        /// </summary>
        public string ConnectionString { get; set; }

        public int ConnectionTimeout => 0;

        public string Database => ConnectionString;

        public ConnectionState State { get; private set; } = ConnectionState.Closed;

        public IDbTransaction BeginTransaction() =>
            throw new NotSupportedException("ParadoxConnection does not support transactions in this version.");

        public IDbTransaction BeginTransaction(IsolationLevel il) => BeginTransaction();

        public void ChangeDatabase(string databaseName) => ConnectionString = databaseName;

        public void Close()
        {
            Executor?.Dispose();
            Executor = null;
            State = ConnectionState.Closed;
        }

        public IDbCommand CreateCommand() => new ParadoxCommand { Connection = this };

        public void Open()
        {
            Executor = new ParadoxSqlExecutor(ConnectionString);
            State = ConnectionState.Open;
        }

        public void Dispose() => Close();
    }
}
