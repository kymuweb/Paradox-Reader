using System;

namespace ParadoxReader
{
    public partial class ParadoxCondition
    {
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
}
