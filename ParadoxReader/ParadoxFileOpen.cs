using System.IO;

namespace ParadoxReader
{
	/// <summary>
	/// Central place for opening table/index/blob files. Inside a read-only scope
	/// (see <see cref="ParadoxTableFile.OpenReadOnly"/>) every file is opened with
	/// FileAccess.Read, so a reader can never write to the table or its indexes.
	/// </summary>
	internal static class ParadoxFileOpen
	{
		[System.ThreadStatic] internal static bool ReadOnlyScope;

		internal static FileStream Open(string path) => ReadOnlyScope
			? new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1)
			: new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 1);
	}
}