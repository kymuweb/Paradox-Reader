# Paradox Reader

A .NET library for reading and writing Borland/Corel **Paradox** database tables (`.DB`, `.PX`, `.Xg#`, `.MB`) without requiring the Borland Database Engine (BDE) to be installed.

Targets **.NET 10**, **.NET Standard 2.0** (covers .NET Framework 4.6.1+ consumers), giving you a dependency-free, cross-platform way to work with legacy Paradox data from modern .NET applications.

## Features

- **Read Paradox tables** — parses table headers, field definitions, and record data directly from `.DB` files.
- **Write support** — append, update, and delete records, with automatic AutoInc field handling.
- **Index management** — reads and maintains primary (`.PX`) and secondary (`.Xg#`) index files, keeping change counters and table versions in sync so BDE doesn't consider the table out of date.
- **BLOB/Memo support** — reads and writes `.MB` blob files for memo and BLOB field types.
- **Table rebuilding** — includes a `TableRebuilder` for rebuilding/repairing tables and indexes, similar to `pdxrbld`.
- **Table creation** — create brand-new Paradox tables and schemas from scratch via `TableCreator`.
- **SQL-like querying** — a lightweight SQL layer (`ParadoxReader.Sql`) provides `ParadoxConnection`/`ParadoxCommand`/`ParadoxDataReader`, including basic joins and `WHERE` clause translation, for querying tables with familiar ADO.NET-style code.
- **Password-protected tables** — supports reading/writing tables secured with a Paradox table password.

## Installation

```powershell
dotnet add package ParadoxReader
```

Or via the NuGet Package Manager in Visual Studio, search for `ParadoxReader`.

## Quick Start

### Reading a table

```csharp
using ParadoxReader;

using var table = new ParadoxTableFile(@"C:\data\CUSTOMER.DB");

foreach (var record in table.Records)
{
	Console.WriteLine(string.Join(", ", record.DataValues));
}
```

### Appending a record

```csharp
using var table = new ParadoxTableFile(@"C:\data\CUSTOMER.DB");
table.AppendRecord(new object[] { 1, "Jane Doe", "jane@example.com" });
```

### Querying with SQL

```csharp
using ParadoxReader.Sql;

using var connection = new ParadoxConnection(@"C:\data");
connection.Open();

using var command = new ParadoxCommand("SELECT * FROM CUSTOMER WHERE Id = @id", connection);
command.Parameters.AddWithValue("@id", 1);

using var reader = command.ExecuteReader();
while (reader.Read())
{
	Console.WriteLine(reader["Name"]);
}
```

### Rebuilding a table

```csharp
using ParadoxReader;

var result = TableRebuilder.Rebuild(@"C:\data\CUSTOMER.DB");
```

## Project Structure

| Project | Description |
|---|---|
| `ParadoxReader` | The core library — packaged and published to NuGet. |
| `ParadoxTest` | Test harness / regression suite exercising the library against sample and corpus data. |
| `ParadoxDesktop` | A Windows Forms desktop app (net461) for browsing Paradox tables. |

## Contributing

Issues and pull requests are welcome. Please open an issue describing the problem or feature before submitting large changes.

## License

MIT
