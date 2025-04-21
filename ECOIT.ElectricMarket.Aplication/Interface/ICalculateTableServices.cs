using ECOIT.ElectricMarket.Application.DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.Interface
{
    public interface ICalculateTableServices
    {
        Task CalculateTableByFormulaAsync(CalculationRequest request);
        Task CalculateCCFDTableAsync(CalculationRequest request);
    }
}
