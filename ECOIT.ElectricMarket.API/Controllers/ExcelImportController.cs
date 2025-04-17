using ECOIT.ElectricMarket.Aplication.Interface;
using ECOIT.ElectricMarket.Application.Interface;
using ECOIT.ElectricMarket.Application.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using OfficeOpenXml;
using System.Data.SqlClient;

namespace ECOIT.ElectricMarket.API.Controllers
{
    [Route("api/excel")]
    [ApiController]
    public class ExcelImportController : ControllerBase
    {
        private readonly ISheetImportHandler _importHandler;
        private readonly ICaculateServices _caculateService;
        private readonly IDynamicTableService _tableService;
        public ExcelImportController(ISheetImportHandler importHandler, ICaculateServices caculate, IDynamicTableService tableService)
        {
            _importHandler = importHandler;
            _caculateService = caculate;
            _tableService = tableService;
        }

        [HttpPost("import-sheet")]
        public async Task<IActionResult> ImportSheet(
            IFormFile file,
            [FromQuery] string selectedSheet,
            [FromQuery] int headerRow,
            [FromQuery] int startRow,
            [FromQuery] string? sheetName,
            [FromQuery] int? endRow = null)
        {
            try
            {
                ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

                if (file == null || file.Length == 0)
                    return BadRequest("File không hợp lệ");

                using var stream = new MemoryStream();
                await file.CopyToAsync(stream);

                var tableName = string.IsNullOrWhiteSpace(sheetName) ? selectedSheet : sheetName;

                await _importHandler.ImportSheetAsync(stream, selectedSheet, headerRow, startRow, tableName, endRow);

                return Ok(new { message = $"Đã import sheet '{selectedSheet}' vào bảng '{tableName}' thành công." });
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }


        [HttpPost("excel")]
        public async Task<IActionResult> GetSheetNames(IFormFile file)
        {
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            if (file == null || file.Length == 0)
                return BadRequest("File Excel không hợp lệ.");

            using var stream = new MemoryStream();
            await file.CopyToAsync(stream);

            ExcelPackage.LicenseContext = OfficeOpenXml.LicenseContext.NonCommercial;

            using var package = new ExcelPackage(stream);
            var sheetNames = package.Workbook.Worksheets
                .Select(ws => ws.Name)
                .ToList();

            return Ok(sheetNames);
        }
        [HttpPost("caculate-fmp")]
        public async Task<IActionResult> CalculateFmp()
        {
            try
            {
                await _caculateService.CalculateFmpAsync();
                return Ok(new { message = "Tính FMP thành công!" });
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }

        [HttpPost("calculate-pm")]
        public async Task<IActionResult> CalculatePM()
        {
            try
            {
                await _caculateService.CaculatePmAsync("FMP", "GiaA0congbo", "SaiKhac(PM)");
                return Ok("Tính FM thành công");
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            };
        }

        [HttpPost("calculate-PM(CFMP)")]
        public async Task<IActionResult> CalculateCFMP()
        {
            try
            {
                await _caculateService.CalculateCFMPAsync();
                return Ok(new { message = "Tính CFMP thành công!" });
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }

        [HttpGet("tables")]
        public async Task<IActionResult> GetTables()
        {
            var tables = await _tableService.GetTableNamesAsync();
            return Ok(tables);
        }

        [HttpGet("data-table")]
        public async Task<IActionResult> GetTableData(string tableName)
        {
            var data = await _tableService.GetTableDataAsync(tableName);
            return Ok(data);
        }

        [HttpPost("calculate-pmcfmp")]
        public async Task<IActionResult> CalculatePmCfmp()
        {
            try
            {
                await _caculateService.CaculatePmCFMPAsync();
                return Ok("Tính pmcfmp thành công");
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            };
        }
    }
}
