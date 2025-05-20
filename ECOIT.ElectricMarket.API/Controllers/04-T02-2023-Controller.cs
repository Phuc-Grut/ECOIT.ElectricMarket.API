using ECOIT.ElectricMarket.Aplication.Interface;
using ECOIT.ElectricMarket.Application.DTO;
using ECOIT.ElectricMarket.Application.Interface;
using ECOIT.ElectricMarket.Application.Services;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;

namespace ECOIT.ElectricMarket.API.Controllers
{
    [Route("api/file4")]
    [ApiController]
    public class _04_T02_2023_Controller : ControllerBase
    {
        private readonly IDynamicTableService _dynamicTableService;
        private readonly IConfiguration _configuration;
        private readonly IFile4Services _file4Services;
        private readonly ICsport _csport;

        public _04_T02_2023_Controller(ICsport csport , IDynamicTableService dynamicTableService, IConfiguration configuration, IFile4Services file4Services)
        {
            _dynamicTableService = dynamicTableService;
            _configuration = configuration;
            _file4Services = file4Services;
            _csport = csport;
        }

        [HttpPost("x1/create")]
        public async Task<IActionResult> TaoBangX1()
        {
            await _dynamicTableService.TaoBangVaTinhX1Async();
            return Ok("Đã tạo bảng X1 và tính đủ 48 giá trị.");
        }

        [HttpPost("calculate-qm1-48chuky")]
        public async Task<IActionResult> CalculateAndInsertQM1()
        {
            var result = await _file4Services.CalculateAndInsertQM1_48ChukyAsync();
            return Ok(result);
        }

        [HttpPost("calculate-qm1-24chuky")]
        public async Task<IActionResult> CalculateAndInsertQM1_24Chuky()
        {
            await _file4Services.CalculateAndInsertQM1_24ChukyAsync();
            return Ok("Tính QM1 24Chuky thành công");
        }

        [HttpPost("calculate-x2-province")]
        public async Task<IActionResult> CalculateAndInsertX2ThaiBinh(string province)
        {
            await _file4Services.CalculateAndInsertX2ProvinceAsync(province);
            return Ok($"Tính x2 {province} thành công");
        }

        [HttpPost("calculate-province")]
        public async Task<IActionResult> CalculateProvince(string province, string tableName)
        {
            await _file4Services.CalculateProvinceAsync(province, tableName);
            return Ok($"Tính {province} thành công");
        }

        [HttpPost("calculate-qm2-province")]
        public async Task<IActionResult> CalculateQM2Province(string province, string tableName)
        {
            await _file4Services.CalculateQM2ProvinceAsync(province, tableName);
            return Ok($"Tính QM2 {province} thành công");
        }

        [HttpPost("calculate-qm2-24ChuKy-province")]
        public async Task<IActionResult> CalculateQM2_24ChuKyProvince(string province, string tableName)
        {
            await _file4Services.CalculateQM2_24ChukyAsync(province, tableName);

            return Ok($"Tính QM2 {province} 24 chu kỳ thành công");
              
        }

        [HttpPost("calculate-qm-tong-hop")]
        public async Task<IActionResult> CalculateQM_TongHop()
        {
            await _file4Services.CalculateQMTongHop(new CalculationRequest
            {
                OutputTable = "QMTongHop",
                OutputLabel = "QMTongHop",
                Formula = "QMTongHop = QM1_48Chuky + QM2_ThaiBinh48Chuky + QM2_VinhTan4 + QM2_VinhTan4MR + QM2_DuyenHai3MR",
                SourceTables = new[] { "QM1_48Chuky", "QM2_ThaiBinh48Chuky", "QM2_VinhTan4", "QM2_VinhTan4MR", "QM2_DuyenHai3MR" }
            });

            return Ok("Tính QM tổng hợp thành công");
        }

        [HttpPost("calculate-csport")]
        public async Task<IActionResult> Calculate_Csport(string tableName, string province)
        {
            await _csport.CalculateCsport1(tableName, province);
            return Ok("Tính Cspot thành công");
        }
    }
}