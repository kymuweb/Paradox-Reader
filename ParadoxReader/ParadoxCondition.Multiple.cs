using System;

namespace ParadoxReader
{
    public partial class ParadoxCondition
    {
        public abstract class Multiple : ParadoxCondition
        {
            protected ParadoxCondition[] SubConditions { get; private set; }

            protected Multiple(ParadoxCondition[] subConditions)
            {
                SubConditions = subConditions;
            }

            public override bool IsDataOk(ParadoxReader.ParadoxRecord dataRec)
            {
                return this.Test(c => c.IsDataOk(dataRec));
            }

            public override bool IsIndexPossible(ParadoxReader.ParadoxRecord indexRec, ParadoxReader.ParadoxRecord nextRec)
            {
                return this.Test(c => c.IsIndexPossible(indexRec, nextRec));
            }

            protected abstract bool Test(Predicate<ParadoxCondition> test);
        }
    }
}
