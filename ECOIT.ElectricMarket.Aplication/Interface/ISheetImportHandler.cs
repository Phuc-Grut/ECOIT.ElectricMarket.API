using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.Interface
{
    public interface ISheetImportHandler
    {
        Task ImportSheetAsync(Stream fileStream, string sheetName, int headerRow, int startRow, string? tableName = null, int? endRow = null);
    }
}
