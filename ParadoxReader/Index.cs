using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ParadoxReader
{
    public class ParadoxPrimaryKey : ParadoxFile
    {
        private readonly ParadoxTable table;

        public ParadoxPrimaryKey(ParadoxTable table, string filePath)
            : base(filePath)
        {
            this.table = table;
        }

        public IEnumerable<ParadoxRecord> Enumerate(ParadoxCondition condition)
        {
            return Enumerate(condition, (ushort)(this.pxRootBlockId-1), this.pxLevelCount);
        }

        private IEnumerable<ParadoxRecord> Enumerate(ParadoxCondition condition, ushort blockId, int indexLevel)
        {
            if (indexLevel == 0)
            {
                var block = this.table.GetBlock(blockId);
                for (int i=0; i<block.RecordCount; i++)
                {
                    var rec = block[i];
                    if (condition.IsDataOk(rec))
                    {
                        yield return rec;
                    }
                }
            }
            else
            {
                var block = this.GetBlock(blockId);
                var blockIdFldIndex = this.FieldCount - 3;
                for (int i = 0; i < block.RecordCount; i++)
                {
                    var rec = block[i];
                    if (condition.IsIndexPossible(rec, i < block.RecordCount-1 ? block[i + 1] : null))
                    {
                        var qry = Enumerate(condition, (ushort)((short) rec.DataValues[blockIdFldIndex]-1), indexLevel - 1);
                        foreach (var dataRec in qry)
                        {
                            yield return dataRec;
                        }
                    }
                }
            }
        }
    }

    public abstract class ParadoxCondition
    {
        public abstract bool IsDataOk(ParadoxRecord dataRec);
        public abstract bool IsIndexPossible(ParadoxRecord indexRec, ParadoxRecord nextRec);

        public class Compare : ParadoxCondition
        {
            public ParadoxCompareOperator Operator { get; private set; }
            public object Value { get; private set; }

            public int DataFieldIndex { get; private set; }
            public int IndexFieldIndex { get; private set; }

            public override bool IsDataOk(ParadoxRecord dataRec)
            {
                var val = dataRec.DataValues[this.DataFieldIndex];
                var comp = Comparer.Default.Compare(val, this.Value);
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
                        throw new NotSupportedException();
                }
            }

            public override bool IsIndexPossible(ParadoxRecord indexRec, ParadoxRecord nextRec)
            {
                var val1 = indexRec.DataValues[this.DataFieldIndex];
                var comp1 = Comparer.Default.Compare(val1, this.Value);
                int comp2;
                if (nextRec != null)
                {
                    var val2 = nextRec.DataValues[this.DataFieldIndex];
                    comp2 = Comparer.Default.Compare(val2, this.Value);
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
                        throw new NotSupportedException();
                }
            }

            public Compare(ParadoxCompareOperator op, object value, int dataFieldIndex, int indexFieldIndex)
            {
                Operator = op;
                Value = value;
                DataFieldIndex = dataFieldIndex;
                IndexFieldIndex = indexFieldIndex;
            }
        }

        public abstract class Multiple : ParadoxCondition
        {
            protected ParadoxCondition[] SubConditions { get; private set; }

            protected Multiple(ParadoxCondition[] subConditions)
            {
                SubConditions = subConditions;
            }

            public override bool IsDataOk(ParadoxRecord dataRec)
            {
                return this.Test(c => c.IsDataOk(dataRec));
            }

            public override bool IsIndexPossible(ParadoxRecord indexRec, ParadoxRecord nextRec)
            {
                return this.Test(c => c.IsIndexPossible(indexRec, nextRec));
            }

            protected abstract bool Test(Predicate<ParadoxCondition> test);
        }

        public class LogicalAnd : Multiple
        {
            public LogicalAnd(params ParadoxCondition[] subConditions) : base(subConditions) { }

            protected override bool Test(Predicate<ParadoxCondition> test)
            {
                foreach (var subCondition in SubConditions)
                {
                    if (!test(subCondition)) return false;
                }
                return true;
            }
        }

        public class LogicalOr : Multiple
        {
            public LogicalOr(params ParadoxCondition[] subConditions) : base(subConditions) { }

            protected override bool Test(Predicate<ParadoxCondition> test)
            {
                foreach (var subCondition in SubConditions)
                {
                    if (test(subCondition)) return true;
                }
                return false;
            }
        }
    }

    public enum ParadoxCompareOperator
    {
        Less,
        LessOrEqual,
        Equal,
        GreaterOrEqual,
        Greater,
        NotEqual
    }
}
