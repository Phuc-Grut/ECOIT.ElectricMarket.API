namespace ECOIT.ElectricMarket.Aplication.Interface
{
    public interface IDynamicTableService
    {
        Task CreateTableAsync(string tableName, List<string> columnNames);

        Task InsertDataAsync(string tableName, List<string> columnNames, List<List<string>> rows);

        Task<List<string>> GetTableNamesAsync();

        Task<List<Dictionary<string, object>>> GetTableDataAsync(string tableName);

        Task TaoBangVaTinhX1Async();
    }
}