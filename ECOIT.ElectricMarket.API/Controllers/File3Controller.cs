using ECOIT.ElectricMarket.Application.DTO;
using ECOIT.ElectricMarket.Application.Interface;
using ECOIT.ElectricMarket.Application.Services;
using Microsoft.AspNetCore.Mvc;

namespace ECOIT.ElectricMarket.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class File3Controller : ControllerBase
    {
        private readonly IFile3Services _file3Services;
        private readonly ICalculateTableServices _calculateTable;

        public File3Controller(IFile3Services file3Services, ICalculateTableServices calculateTable)
        {
            _file3Services = file3Services;
            _calculateTable = calculateTable;
        }

        [HttpPost("Tinh3Gia")]
        public async Task<IActionResult> Tinh3Gia(string tableName)
        {
            await _file3Services.Tinh3Gia(tableName);
            return Ok($"Tính 3 giá {tableName} thành công");
        }

        [HttpPost("Tinh-Qbst+Qntt")]
        public async Task<IActionResult> TinhQbstQntt()
        {
            await _file3Services.CalculateMultiTableFormulaAsync
            (
                outputTable: "Qbst_QNTT",
                outputLabel: "Qbst_QNTT",
                formula: "Qbst_QNTT = QM1_PhuTaiENV - QM1_48Chuky - QM2_ThaiBinh - QM2_VinhTan4 - QM2_VinhTan4MR - QM2_DuyenHai3MR",
                sourceTables: new List<string>
                {
                    "QM1_PhuTaiENV",
                    "QM1_48Chuky",
                    "QM2_ThaiBinh",
                    "QM2_VinhTan4",
                    "QM2_VinhTan4MR",
                    "QM2_DuyenHai3MR"
                }
            );

            return Ok($"Tính Qbst+Qntt thành công");
        }

        [HttpPost("SanLuongChuNhat-Qbst + Qntt")]

        public async Task<IActionResult> SanLuongChuNhatQbstQntt()
        {
            await _file3Services.ExtractSundayRowsAsync("Qbst_QNTT", "SanLuongChuNhat_Qbst_Qntt");
            return Ok("Đã trích xuất các hàng Chủ Nhật từ Qbst_QNTT sang SanLuongChuNhat_Qbst_Qntt.");
        }

        [HttpPost("Calculate-SanluongNTT")]
        public async Task<IActionResult> SanluongNTT()
        {
            try
            {
                await _file3Services.CalculateSanluongNTT(
                    table1: "QL_BST",
                    table2: "TylemuaNTT",
                    outputTable: "SanluongNTT");

                return Ok("Đã tính xong FMP");
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }

        [HttpPost("Calculate-SanluongBTS")]
        public async Task<IActionResult> SanluongBTS()
        {
            try
            {
                await _file3Services.CalculateSanluongBTS
                (
                    table1: "Qbst_QNTT",
                    table2: "SanluongNTT",
                    outputTable: "SanluongBTS"
                );

                return Ok("Đã tính xong SanluongBTS");
            }
            catch (Exception ex)
            {
                return BadRequest(new { error = ex.Message });
            }
        }

        [HttpPost("create-he-so-k")]
        public async Task<IActionResult> CreateHeSoK()
        {
            await _file3Services.CreateHeSoKAsync();
            return Ok("Lấy hệ số k thành công");
        }

    }
}