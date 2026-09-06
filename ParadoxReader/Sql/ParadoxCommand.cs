using System;
using System.Data;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// Minimal <see cref="IDbCommand"/> wrapper executing SQL text via
    /// <see cref="ParadoxSqlExecutor"/>. Supports named ("@name") and
    /// positional ("?") parameter placeholders in <see cref="CommandText"/>;
    /// bind values via <see cref="Parameters"/> before executing.
    /// </summary>
    public sealed class ParadoxCommand : IDbCommand
    {
        public ParadoxCommand() { }

        public ParadoxCommand(string commandText, ParadoxConnection connection)
        {
            CommandText = commandText;
            Connection = connection;
        }

        public string CommandText { get; set; }

        public int CommandTimeout { get; set; }

        public CommandType CommandType { get; set; } = CommandType.Text;

        IDbConnection IDbCommand.Connection
        {
            get => Connection;
            set => Connection = (ParadoxConnection)value;
        }

        public ParadoxConnection Connection { get; set; }

        public IDataParameterCollection Parameters => ParadoxParameters;

        /// <summary>Strongly-typed access to this command's bound parameters.</summary>
        public ParadoxParameterCollection ParadoxParameters { get; } = new ParadoxParameterCollection();

        public IDbTransaction Transaction { get; set; }

        public UpdateRowSource UpdatedRowSource { get; set; }

        public void Cancel() { }

        public IDbDataParameter CreateParameter() => new ParadoxParameter();

        public void Dispose() { }

        public int ExecuteNonQuery()
        {
            EnsureExecutor();
            return Connection.Executor.ExecuteNonQuery(CommandText, ParadoxParameters.ToLookup());
        }

        public IDataReader ExecuteReader() => ExecuteReader(CommandBehavior.Default);

        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            EnsureExecutor();
            return Connection.Executor.ExecuteReader(CommandText, ParadoxParameters.ToLookup());
        }

        public object ExecuteScalar()
        {
            using (var reader = ExecuteReader())
            {
                if (reader.Read() && reader.FieldCount > 0)
                    return reader.GetValue(0);
                return null;
            }
        }

        public void Prepare() { }

        private void EnsureExecutor()
        {
            if (Connection?.Executor == null)
                throw new InvalidOperationException("ParadoxConnection must be Open() before executing a command.");
        }
    }
}
