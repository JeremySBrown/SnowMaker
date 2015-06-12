namespace SnowMaker
{
    public interface IUniqueIdGenerator
    {
        long NextId(string scopeName);
        long LastId(string scopeName);
        void SetSeed(string scopeName, long seed);
    }
}