using ECOIT.ElectricMarket.Aplication.Interface;
using Microsoft.AspNetCore.Mvc;

namespace ECOIT.ElectricMarket.API.Controllers
{
    [Route("api/file4")]
    [ApiController]
    public class _04_T02_2023_Controller : ControllerBase
    {
        private readonly IDynamicTableService _dynamicTableService;

        public _04_T02_2023_Controller(IDynamicTableService dynamicTableService)
        {
            _dynamicTableService = dynamicTableService;
        }

        [HttpPost("x1/create")]
        public async Task<IActionResult> TaoBangX1()
        {
            await _dynamicTableService.TaoBangVaTinhX1Async();
            return Ok("Đã tạo bảng X1 và tính đủ 48 giá trị.");
        }
    }
}