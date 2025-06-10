using ECOIT.ElectricMarket.Application.Interface;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace ECOIT.ElectricMarket.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class file0Controller : ControllerBase
    {
        private readonly IFile0Services _file0Services;
        public file0Controller(IFile0Services file0Services)
        {
            _file0Services = file0Services;
        }

        [HttpPost("TinhTong")]
        public async Task<IActionResult> TinhTong()
        {
            try
            {
                await _file0Services.TinhTong();
                return Ok("Tính tổng thành công");
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, $"Lỗi: {ex.Message}");
            }
        }
    }
}
