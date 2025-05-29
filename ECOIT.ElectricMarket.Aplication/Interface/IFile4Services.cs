using ECOIT.ElectricMarket.Application.DTO;

namespace ECOIT.ElectricMarket.Application.Interface
{
    public interface IFile4Services
    {
        Task<string> CalculateAndInsertQM1_48ChukyAsync();

        Task CalculateAndInsertQM1_24ChukyAsync();

        Task CalculateAndInsertX2ProvinceAsync(string province);

        Task CalculateProvinceAsync(string province, string tableName);

        Task CalculateQM2ProvinceAsync(string province, string tableName);

        Task CalculateQM2_24ChukyAsync(string province, string tableName);

        Task CalculateQMTongHop(CalculationRequest request);
    }
}