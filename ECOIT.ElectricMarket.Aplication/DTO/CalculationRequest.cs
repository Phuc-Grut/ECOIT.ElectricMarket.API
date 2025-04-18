using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.DTO
{
    public class CalculationRequest
    {
        public string OutputTable { get; set; } = string.Empty;
        public string OutputLabel { get; set; } = string.Empty;
        public string Formula { get; set; } = string.Empty; // Ví dụ: "FMP = SMP + CAN"
        public string[] SourceTables { get; set; } = Array.Empty<string>();
    }
}
