﻿using ECOIT.ElectricMarket.Aplication.Interface;
using ECOIT.ElectricMarket.Application.DTO;
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
        private readonly ICalculateTableServices _calculateTableServices;
        public ExcelImportController(ISheetImportHandler importHandler, ICaculateServices caculate, IDynamicTableService tableService, ICalculateTableServices calculateTableServices)
        {
            _importHandler = importHandler;
            _caculateService = caculate;
            _tableService = tableService;
            _calculateTableServices = calculateTableServices;
        }

        [HttpPost("import-sheet")]
        public async Task<IActionResult> ImportSheet
        (
            IFormFile file,
            [FromQuery] string selectedSheet,
            [FromQuery] int headerRow,
            [FromQuery] int startRow,
            [FromQuery] string? sheetName,
            [FromQuery] int? endRow = null,
            [FromQuery] int? maxColCount = null
        )
        {
            try
            {
                ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

                if (file == null || file.Length == 0)
                    return BadRequest("File không hợp lệ");

                using var stream = new MemoryStream();
                await file.CopyToAsync(stream);

                var tableName = string.IsNullOrWhiteSpace(sheetName) ? selectedSheet : sheetName;

                await _importHandler.ImportSheetAsync(stream, selectedSheet, headerRow, startRow, tableName, endRow, maxColCount);

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
        //[HttpPost("caculate-fmp")]
        //public async Task<IActionResult> CalculateFmp()
        //{
        //    try
        //    {
        //        await _caculateService.CalculateFmpAsync();
        //        return Ok(new { message = "Tính FMP thành công!" });
        //    }
        //    catch (Exception ex)
        //    {
        //        return BadRequest(new { error = ex.Message });
        //    }
        //}

        [HttpPost("caculate-fmp")]
        public async Task<IActionResult> CalculateFmp()
        {
            try
            {
                await _calculateTableServices.CalculateTableByFormulaAsync(new CalculationRequest
                {
                    OutputTable = "FMP",
                    OutputLabel = "FMP",
                    Formula = "FMP = SMP + CAN",
                    SourceTables = new[] { "NhapgiaNM" }
                });

                return Ok(" Đã tính xong FMP");
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

        //[HttpPost("caculate-ccfd-pm1")]
        //public async Task<IActionResult> CalculateCCFDPM1()
        //{
        //    try
        //    {
        //        await _calculateTableServices.CalculateCCFDTableAsync(new CalculationRequest
        //        {
        //            OutputTable = "CCFD_PM1",
        //            Formula = "CCFD = QC * (PC - FMP) / 1000",
        //            SourceTables = new[] { "QC_PM1", "PC_PM1", "FMP" }
        //        });

        //        return Ok(" Đã tính xong CCFD");
        //    }
        //    catch (Exception ex)
        //    {
        //        return BadRequest(new { error = ex.Message });
        //    }
        //}

        //[HttpPost("caculate-ccfd-pm4")]
        //public async Task<IActionResult> CalculateCCFDPM4()
        //{
        //    try
        //    {
        //        await _calculateTableServices.CalculateCCFDTableAsync(new CalculationRequest
        //        {
        //            OutputTable = "CCFD_PM4",
        //            Formula = "CCFD = QC * (PC - FMP) / 1000",
        //            SourceTables = new[] { "QC_PM4", "PC_PM4", "FMP" }
        //        });

        //        return Ok(" Đã tính xong CCFD");
        //    }
        //    catch (Exception ex)
        //    {
        //        return BadRequest(new { error = ex.Message });
        //    }
        //}

        //[HttpPost("caculate-ccfd-tb1")]
        //public async Task<IActionResult> CalculateCCFDTB1()
        //{
        //    try
        //    {
        //        await _calculateTableServices.CalculateCCFDTableAsync(new CalculationRequest
        //        {
        //            OutputTable = "CCFD_TB1",
        //            Formula = "CCFD = QC * (PC - FMP) / 1000",
        //            SourceTables = new[] { "QC_TB1", "PC_TB1", "FMP" }
        //        });

        //        return Ok(" Đã tính xong CCFD");
        //    }
        //    catch (Exception ex)
        //    {
        //        return BadRequest(new { error = ex.Message });
        //    }
        //}
        //[HttpPost("caculate-ccfd-dh3")]
        //public async Task<IActionResult> CalculateCCFDDH3()
        //{
        //    try
        //    {
        //        await _calculateTableServices.CalculateCCFDTableAsync(new CalculationRequest
        //        {
        //            OutputTable = "CCFD_DH3",
        //            Formula = "CCFD = QC * (PC - FMP) / 1000",
        //            SourceTables = new[] { "QC_DH3", "PC_DH3", "FMP" }
        //        });

        //        return Ok(" Đã tính xong CCFD");
        //    }
        //    catch (Exception ex)
        //    {
        //        return BadRequest(new { error = ex.Message });
        //    }
        //}

        [HttpPost("caculate-ccfd/{type}")]
        public async Task<IActionResult> CalculateCCFD(string type)
        {
            try
            {
                var upperType = type.ToUpper();

                await _calculateTableServices.CalculateCCFDTableAsync(new CalculationRequest
                {
                    OutputTable = $"CCFD_{upperType}",
                    Formula = "CCFD = QC * (PC - FMP) / 1000",
                    SourceTables = new[] { $"QC_{upperType}", $"PC_{upperType}", "FMP" }
                });

                return Ok($"Đã tính xong CCFD cho {upperType}");
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }

        //[HttpPost("calculate-san-luong-hop-dong")]
        //public async Task<IActionResult> CalculateSanLuong([FromQuery] string outputTable = "SanLuongHopDong")
        //{
        //    await _calculateTableServices.CalculateSanLuongHopDongAsync(outputTable);
        //    return Ok($"Đã tạo bảng '{outputTable}' thành công.");
        //}

        //[HttpPost("calculate-chi-phi")]
        //public async Task<IActionResult> CalculateChiPhi([FromQuery] string outputTable = "ChiPhi")
        //{
        //    await _calculateTableServices.CalculateChiPhiAsync(outputTable);
        //    return Ok($"Đã tạo bảng '{outputTable}' thành công.");
        //}

        [HttpPost("calculate-tonghop")]
        public async Task<IActionResult> CalculateTongHop()
        {
            await _calculateTableServices.CalculateTongHopAsync();
            return Ok();
        }
    }
}
