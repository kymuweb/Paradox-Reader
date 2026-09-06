using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace ParadoxReader.Sql
{
    /// <summary>
    /// <see cref="IDataParameterCollection"/> implementation backing
    /// <see cref="ParadoxCommand.Parameters"/>. Holds <see cref="ParadoxParameter"/>
    /// instances added by the caller before <see cref="ParadoxCommand.ExecuteReader()"/>/
    /// ExecuteNonQuery/ExecuteScalar are called.
    /// </summary>
    public sealed class ParadoxParameterCollection : IDataParameterCollection
    {
        private readonly List<ParadoxParameter> items = new List<ParadoxParameter>();

        public object this[string parameterName]
        {
            get => Find(parameterName)?.Value;
            set
            {
                var p = Find(parameterName);
                if (p == null) throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found.");
                p.Value = value;
            }
        }

        public object this[int index]
        {
            get => items[index];
            set => items[index] = (ParadoxParameter)value;
        }

        public bool Contains(string parameterName) => Find(parameterName) != null;

        public int IndexOf(string parameterName)
        {
            for (int i = 0; i < items.Count; i++)
                if (NameMatches(items[i], parameterName)) return i;
            return -1;
        }

        public void RemoveAt(string parameterName)
        {
            int idx = IndexOf(parameterName);
            if (idx >= 0) items.RemoveAt(idx);
        }

        public int Add(object value)
        {
            items.Add((ParadoxParameter)value);
            return items.Count - 1;
        }

        public void Clear() => items.Clear();
        public bool Contains(object value) => items.Contains(value as ParadoxParameter);
        public int IndexOf(object value) => items.IndexOf(value as ParadoxParameter);
        public void Insert(int index, object value) => items.Insert(index, (ParadoxParameter)value);
        public void Remove(object value) => items.Remove(value as ParadoxParameter);
        public void RemoveAt(int index) => items.RemoveAt(index);

        public int Count => items.Count;
        public bool IsFixedSize => false;
        public bool IsReadOnly => false;
        public bool IsSynchronized => false;
        public object SyncRoot => this;

        public void CopyTo(Array array, int index) => ((ICollection)items).CopyTo(array, index);
        public IEnumerator GetEnumerator() => items.GetEnumerator();

        /// <summary>
        /// Adds a new <see cref="ParadoxParameter"/> with the given name/value
        /// and returns it, mirroring the common ADO.NET provider convenience
        /// overload (e.g. SqlCommand.Parameters.AddWithValue).
        /// </summary>
        public ParadoxParameter AddWithValue(string parameterName, object value)
        {
            var p = new ParadoxParameter(parameterName, value);
            items.Add(p);
            return p;
        }

        /// <summary>
        /// Builds a name-&gt;value lookup for statement execution, matching
        /// each bound parameter's normalized name (see
        /// <see cref="ParadoxParameter.NormalizedName"/>) or, for positional
        /// '?' placeholders parsed as synthetic "?N" names, by occurrence
        /// order within this collection.
        /// </summary>
        internal IDictionary<string, object> ToLookup()
        {
            var dict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            int positionalIndex = 0;
            foreach (var p in items)
            {
                if (!string.IsNullOrEmpty(p.NormalizedName))
                    dict[p.NormalizedName] = p.Value;
                // Also register under the synthetic positional key so '?' placeholders
                // (parsed as "?0", "?1", ... in declaration order) resolve even when the
                // caller didn't set ParameterName, matching common "unnamed" parameter usage.
                dict["?" + positionalIndex] = p.Value;
                positionalIndex++;
            }
            return dict;
        }

        private ParadoxParameter Find(string parameterName)
        {
            foreach (var p in items)
                if (NameMatches(p, parameterName)) return p;
            return null;
        }

        private static bool NameMatches(ParadoxParameter p, string parameterName)
        {
            if (string.Equals(p.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase)) return true;
            string normalizedTarget = !string.IsNullOrEmpty(parameterName) && (parameterName[0] == '@' || parameterName[0] == ':')
                ? parameterName.Substring(1) : parameterName;
            return string.Equals(p.NormalizedName, normalizedTarget, StringComparison.OrdinalIgnoreCase);
        }
    }
}
