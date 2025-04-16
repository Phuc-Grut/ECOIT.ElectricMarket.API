using ECOIT.ElectricMarket.Application.Interface;
using ECOIT.ElectricMarket.Application.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using OfficeOpenXml;

namespace ECOIT.ElectricMarket.API.Controllers
{
    [Route("api/excel")]
    [ApiController]
    public class ExcelImportController : ControllerBase
    {
        private readonly SheetImportHandler _importHandler;
        private readonly ICaculateServices _caculateService;
        public ExcelImportController(SheetImportHandler importHandler, ICaculateServices caculate)
        {
            _importHandler = importHandler;
            _caculateService = caculate;
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
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            if (file == null || file.Length == 0)
                return BadRequest("File không hợp lệ");

            using var stream = new MemoryStream();
            await file.CopyToAsync(stream);

            var tableName = string.IsNullOrWhiteSpace(sheetName) ? selectedSheet : sheetName;

            await _importHandler.ImportSheetAsync(stream, selectedSheet, headerRow, startRow, tableName, endRow);

            return Ok(new { message = $"Đã import sheet '{selectedSheet}' vào bảng '{tableName}' thành công." });
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
        [HttpPost("calculate-fmp")]
        public async Task<IActionResult> CalculateFmp()
        {
            await _caculateService.CalculateFmpAsync();
            return Ok("Đã tính toán và lưu FMP vào bảng thành công.");
        }

        [HttpPost("calculate-pm")]
        public async Task<IActionResult> CalculatePM()
        {
            await _caculateService.CaculatePmAsync("FMP", "GiáA0côngbố", "SaiKhac");
            return Ok("Đã tính toán và lưu FMP vào bảng thành công.");
        }

    }
}
