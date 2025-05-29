namespace ECOIT.ElectricMarket.Application.Interface
{
    public interface IFile3Services
    {
        Task Tinh3Gia(string tableName);
        Task CalculateMultiTableFormulaAsync(string outputTable, string outputLabel, string formula, List<string> sourceTables);
        Task ExtractSundayRowsAsync(string sourceTable, string targetTable);
    }
}