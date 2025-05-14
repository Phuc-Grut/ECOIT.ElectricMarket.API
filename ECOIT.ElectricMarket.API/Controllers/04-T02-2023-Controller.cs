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
            string connectionString = _configuration.GetConnectionString("DefaultConnection");

            using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();

            var dtSource = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT * FROM QM1_48Chuky", conn))
            {
                adapter.Fill(dtSource);
            }

            // Tạo bảng kết quả
            var dtResult = new DataTable();
            dtResult.Columns.Add("Chukì");
            dtResult.Columns.Add("Col2");
            for (int i = 1; i <= 24; i++)
            {
                dtResult.Columns.Add($"{i}h");
            }
            dtResult.Columns.Add("Tổng");

            foreach (DataRow row in dtSource.Rows)
            {
                var newRow = dtResult.NewRow();

                newRow["Chukì"] = row["Chukì"];
                newRow["Col2"] = row["Col2"];

                if (row["Chukì"]?.ToString() == "Ngày" || row["Col2"]?.ToString() == "Thứ")
                {
                    for (int i = 1; i <= 24; i++)
                    {
                        newRow[$"{i}h"] = i.ToString();
                    }
                    newRow["Tổng"] = "Tổng";
                    dtResult.Rows.Add(newRow);
                    continue;
                }

                decimal total = 0;

                for (int i = 0; i < 24; i++)
                {
                    int colIndex1 = 2 + (i * 2);
                    int colIndex2 = 3 + (i * 2);

                    var val1 = row[colIndex1]?.ToString();
                    var val2 = row[colIndex2]?.ToString();

                    decimal.TryParse(val1, out decimal d1);
                    decimal.TryParse(val2, out decimal d2);

                    decimal sum = d1 + d2;
                    total += sum;

                    newRow[$"{i + 1}h"] = Math.Round(sum).ToString("N0", CultureInfo.InvariantCulture);
                }

                newRow["Tổng"] = Math.Round(total).ToString("N0", CultureInfo.InvariantCulture);
                dtResult.Rows.Add(newRow);
            }

            // Tạo bảng nếu chưa tồn tại
            string createTableSql = @"
            IF OBJECT_ID('dbo.QM1_24Chuky', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.QM1_24Chuky (
                    Chukì NVARCHAR(MAX),
                    Col2 NVARCHAR(MAX),
                    " + string.Join(",", Enumerable.Range(1, 24).Select(i => $"[{i}h] NVARCHAR(MAX)")) + @",
                    Tổng NVARCHAR(MAX)
                    )
             END";

            using (var cmdCreate = new SqlCommand(createTableSql, conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            // Xoá dữ liệu cũ
            using (var truncateCmd = new SqlCommand("TRUNCATE TABLE dbo.QM1_24Chuky", conn))
            {
                await truncateCmd.ExecuteNonQueryAsync();
            }

            // Đẩy dữ liệu mới
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = "dbo.QM1_24Chuky";
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dtResult);
            }

            return Ok("Tính QM1 24 chu kỳ thành công");
        }

        [HttpPost("calculate-x2-thaibinh")]
        public async Task<IActionResult> CalculateAndInsertX2ThaiBinh()
        {
            string connectionString = _configuration.GetConnectionString("DefaultConnection");

            using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();

            var dtSchema = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT TOP 1 * FROM QM1_48Chuky", conn))
            {
                adapter.FillSchema(dtSchema, SchemaType.Source);
            }

            var dtResult = dtSchema.Clone();

            var dtSource = new DataTable();

            using (var adapter = new SqlDataAdapter("SELECT * FROM X2 WHERE [Đơnvị] = N'Thái Bình'", conn))
            {
                adapter.Fill(dtSource);
            }

            var dtNgayThu = new DataTable();
            using (var ngayAdapter = new SqlDataAdapter("SELECT Chukì, Col2 FROM QM1_48Chuky WHERE Chukì <> 'Ngày'", conn))
            {
                ngayAdapter.Fill(dtNgayThu);
            }

            foreach (DataRow row in dtSource.Rows)
            {
                var newRow = dtResult.NewRow();

                int rowIndex = dtResult.Rows.Count;
                if (rowIndex < dtNgayThu.Rows.Count)
                {
                    newRow["Chukì"] = dtNgayThu.Rows[rowIndex]["Chukì"];
                    newRow["Col2"] = dtNgayThu.Rows[rowIndex]["Col2"];
                }
                else
                {
                    newRow["Chukì"] = "";
                    newRow["Col2"] = "";
                };

                foreach (DataColumn col in dtResult.Columns)
                {
                    var colName = col.ColumnName;
                    if (colName == "Chukì" || colName == "Col2")
                        continue;

                    string srcCol = dtSource.Columns
                        .Cast<DataColumn>()
                        .FirstOrDefault(c => c.ColumnName.EndsWith("_" + colName))?.ColumnName;

                    if (!string.IsNullOrEmpty(srcCol) && dtSource.Columns.Contains(srcCol))
                    {
                        var val = row[srcCol];
                        newRow[colName] = double.TryParse(val?.ToString(), out var v) ? v : 0;
                    }
                }

                dtResult.Rows.Add(newRow);
            }

            string createTableSql = GenerateCreateTableSql("X2_ThaiBinh", dtResult);

            using (var cmdCreate = new SqlCommand(createTableSql, conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            using (var cmdTruncate = new SqlCommand("TRUNCATE TABLE X2_ThaiBinh", conn))
            {
                await cmdTruncate.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = "X2_ThaiBinh";
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dtResult);
            }

            return Ok("Đã xử lý và lưu bảng X2_ThaiBinh thành công.");
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