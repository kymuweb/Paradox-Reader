namespace ParadoxReader
{
    public partial class ParadoxCondition
    {
        public class Compare : ParadoxCondition
        {
            public ParadoxCompareOperator Operator { get; private set; }
            public object Value { get; private set; }

            public int DataFieldIndex { get; private set; }
            public int IndexFieldIndex { get; private set; }

            public override bool IsDataOk(ParadoxReader.ParadoxRecord dataRec)
            {
                var val = dataRec.DataValues[this.DataFieldIndex];
                var comp = CompareValues(val, this.Value);
                switch (Operator)
                {
                    case ParadoxCompareOperator.Equal:
                        return comp == 0;
                    case ParadoxCompareOperator.NotEqual:
                        return comp != 0;
                    case ParadoxCompareOperator.Greater:
                        return comp > 0;
                    case ParadoxCompareOperator.GreaterOrEqual:
                        return comp >= 0;
                    case ParadoxCompareOperator.Less:
                        return comp < 0;
                    case ParadoxCompareOperator.LessOrEqual:
                        return comp <= 0;
                    default:
                        throw new System.NotSupportedException();
                }
            }

            public override bool IsIndexPossible(ParadoxReader.ParadoxRecord indexRec, ParadoxReader.ParadoxRecord nextRec)
            {
                // indexRec/nextRec here represent a synthesized record built directly
                // from an index leaf entry's key data (see SecondaryIndexFile.Enumerate
                // and ParadoxPrimaryKey.Enumerate). For secondary indexes, the composed
                // key layout differs from the parent table's field layout (indexed
                // field(s) followed by appended primary-key fields), so IndexFieldIndex
                // - not DataFieldIndex - must be used to look up the value in the key's
                // own coordinate system. For the primary index, IndexFieldIndex is
                // conventionally the same as DataFieldIndex.
                var val1 = indexRec.DataValues[this.IndexFieldIndex];
                var comp1 = CompareValues(val1, this.Value);
                int comp2;
                if (nextRec != null)
                {
                    var val2 = nextRec.DataValues[this.IndexFieldIndex];
                    comp2 = CompareValues(val2, this.Value);
                }
                else
                {
                    comp2 = 1; // last index range ends in infinite
                }
                switch (Operator)
                {
                    case ParadoxCompareOperator.Equal:
                        return comp1 <= 0 && comp2 >= 0;
                    case ParadoxCompareOperator.NotEqual:
                        return comp1 > 0 || comp2 < 0;
                    case ParadoxCompareOperator.Greater:
                        return comp2 > 0;
                    case ParadoxCompareOperator.GreaterOrEqual:
                        return comp2 >= 0;
                    case ParadoxCompareOperator.Less:
                        return comp1 < 0;
                    case ParadoxCompareOperator.LessOrEqual:
                        return comp1 <= 0;
                    default:
                        throw new System.NotSupportedException();
                }
            }

            /// <summary>
            /// Compares two values the same way Paradox physically orders keys
            /// on disk. Strings must be compared ordinally (raw byte value),
            /// not with <see cref="System.Collections.Comparer.Default"/>'s
            /// culture-aware string comparison - the latter can disagree with
            /// the on-disk B-tree order for bytes like 0x80-0xFF (mapped to
            /// punctuation/symbols in the default codepage), causing index
            /// traversal to prune the wrong subtree and miss matching rows.
            /// </summary>
            private static int CompareValues(object a, object b)
            {
                if (a is string sa && b is string sb)
                    return string.CompareOrdinal(sa, sb);
                return System.Collections.Comparer.Default.Compare(a, b);
            }

            public Compare(ParadoxCompareOperator op, object value, int dataFieldIndex, int indexFieldIndex)
            {
                Operator = op;
                Value = value;
                DataFieldIndex = dataFieldIndex;
                IndexFieldIndex = indexFieldIndex;
            }
        }
    }
}
