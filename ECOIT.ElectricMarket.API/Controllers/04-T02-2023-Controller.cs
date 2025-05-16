using ECOIT.ElectricMarket.Aplication.Interface;
using ECOIT.ElectricMarket.Application.Interface;
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

        public _04_T02_2023_Controller(IDynamicTableService dynamicTableService, IConfiguration configuration, IFile4Services file4Services)
        {
            _dynamicTableService = dynamicTableService;
            _configuration = configuration;
            _file4Services = file4Services;
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

        [HttpPost("calculate-x2-thaibinh")]
        public async Task<IActionResult> CalculateAndInsertX2ThaiBinh()
        {
            await _file4Services.CalculateAndInsertX2ThaiBinhAsync();
            return Ok("Tính x2 thái bình thành công");
        }

        private string GenerateCreateTableSql(string tableName, DataTable schema)
        {
            var columns = new List<string>();

            foreach (DataColumn col in schema.Columns)
            {
                string colName = $"[{col.ColumnName}]";
                string sqlType = "NVARCHAR(MAX)";

                if (col.DataType == typeof(int)) sqlType = "INT";
                else if (col.DataType == typeof(double) || col.DataType == typeof(float) || col.DataType == typeof(decimal)) sqlType = "FLOAT";
                else if (col.DataType == typeof(DateTime)) sqlType = "DATETIME";

                columns.Add($"{colName} {sqlType}");
            }

            return $@"
        IF OBJECT_ID('dbo.{tableName}', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.{tableName} (
                {string.Join(",\n", columns)}
            )
        END";
        }
    }
}