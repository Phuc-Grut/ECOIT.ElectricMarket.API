using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.Interface
{
    public interface ICsport
    {
        Task CalculateCsport1(string tbaleName, string province);
    }
}
