using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.Interface
{
    public interface IFile4Services
    {
        Task<string> CalculateAndInsertQM1_48ChukyAsync();
    }
}
