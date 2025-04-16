using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Aplication.Interface
{
    public interface IDynamicTableService
    {
        Task CreateTableAsync(string tableName, List<string> columnNames);
        Task InsertDataAsync(string tableName, List<string> columnNames, List<List<string>> rows);
    }
}
