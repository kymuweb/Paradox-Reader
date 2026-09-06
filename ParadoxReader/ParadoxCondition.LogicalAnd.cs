using System;

namespace ParadoxReader
{
    public partial class ParadoxCondition
    {
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
    }
}
