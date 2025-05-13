using ECOIT.ElectricMarket.Aplication.Interface;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace ECOIT.ElectricMarket.API.Controllers
{
    [Route("api/file4")]
    [ApiController]
    public class _04_T02_2023_Controller : ControllerBase
    {
        private readonly IDynamicTableService _dynamicTableService;
        private readonly IConfiguration _configuration;

        public _04_T02_2023_Controller(IDynamicTableService dynamicTableService, IConfiguration configuration)
        {
            _dynamicTableService = dynamicTableService;
            _configuration = configuration;
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
            string connectionString = _configuration.GetConnectionString("DefaultConnection");

            using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();

            var dtSchema = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT TOP 1 * FROM QM1_PhuTaiENV", conn))
            {
                adapter.FillSchema(dtSchema, SchemaType.Source);
            }

            var columnDefs = dtSchema.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");

            string createTableSql = $@"
            IF OBJECT_ID('dbo.QM1_48Chuky', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.QM1_48Chuky (
                    {string.Join(",", columnDefs)}
                )
            END";

            using (var createCmd = new SqlCommand(createTableSql, conn))
            {
                await createCmd.ExecuteNonQueryAsync();
            }

            // 3. Truncate bảng QM1 nếu đã có
            using (var truncateCmd = new SqlCommand("TRUNCATE TABLE dbo.QM1_48Chuky", conn))
            {
                await truncateCmd.ExecuteNonQueryAsync();
            }

            // 4. Load hệ số X1
            var x1Dict = new Dictionary<string, decimal>();
            using (var cmd = new SqlCommand("SELECT Gio, X1 FROM X1", conn))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    x1Dict[reader.GetString(0)] = reader.GetDecimal(1);
                }
            }

            // 5. Load dữ liệu gốc
            var dtSource = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT * FROM QM1_PhuTaiENV", conn))
            {
                adapter.Fill(dtSource);
            }

            // 6. Tạo kết quả
            var dtResult = dtSource.Clone();

            foreach (DataRow row in dtSource.Rows)
            {
                var newRow = dtResult.NewRow();

                foreach (DataColumn col in dtSource.Columns)
                {
                    string colName = col.ColumnName;

                    if (colName == "Chuki" || colName == "Col2")
                    {
                        newRow[colName] = row[colName]?.ToString();
                    }
                    else
                    {
                        var cell = row[colName]?.ToString();
                        if (decimal.TryParse(cell, out decimal value) &&
                            x1Dict.TryGetValue(colName, out decimal x1Value))
                        {
                            newRow[colName] = Math.Round(value * x1Value, 2).ToString();
                        }
                        else
                        {
                            newRow[colName] = cell;
                        }
                    }
                }

                dtResult.Rows.Add(newRow);
            }

            // 7. Insert bằng SqlBulkCopy
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = "dbo.QM1_48Chuky";

                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dtResult);
            }

            return Ok("Tính QM1 thành công");
        }

    }
}