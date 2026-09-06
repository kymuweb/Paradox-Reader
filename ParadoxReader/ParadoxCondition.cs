namespace ParadoxReader
{
    public abstract partial class ParadoxCondition
    {
        public abstract bool IsDataOk(ParadoxReader.ParadoxRecord dataRec);
        public abstract bool IsIndexPossible(ParadoxReader.ParadoxRecord indexRec, ParadoxReader.ParadoxRecord nextRec);
    }
}
