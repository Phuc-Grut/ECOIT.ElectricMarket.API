using ECOIT.ElectricMarket.Application.Interface;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace ECOIT.ElectricMarket.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class File3Controller : ControllerBase
    {
        private readonly IFile3Services _file3Services;

        public File3Controller(IFile3Services file3Services)
        {
            _file3Services = file3Services;
        }

        [HttpPost("Tinh3Gia")]
        public async Task<IActionResult> Tinh3Gia(string tableName)
        {
            await _file3Services.Tinh3Gia(tableName);
            return Ok($"Tính 3 giá {tableName} thành công");
        }
    }
}
