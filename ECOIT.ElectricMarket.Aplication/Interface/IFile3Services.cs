using ECOIT.ElectricMarket.Application.DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.Interface
{
    public interface IFile3Services
    {
        Task Tinh3Gia(string tableName);
    }
}
