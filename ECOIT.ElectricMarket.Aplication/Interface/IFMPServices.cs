using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.Interface
{
    public interface ICaculateServices
    {
        Task CalculateFmpAsync();
        Task CaculatePmAsync(string tableFMP, string tableA0, string resultTableName);
        Task CalculateCFMPAsync();
        Task CaculatePmCFMPAsync();
    }
}
