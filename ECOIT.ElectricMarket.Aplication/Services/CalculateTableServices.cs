using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ECOIT.ElectricMarket.Application.DTO;
using Microsoft.Extensions.Configuration;
using ECOIT.ElectricMarket.Application.Interface;

namespace ECOIT.ElectricMarket.Application.Services
{
    public class CalculateTableServices : ICalculateTableServices
    {
        private readonly string _connectionString;
        public CalculateTableServices(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection");
        }
        public async Task CalculateTableByFormulaAsync(CalculationRequest request)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            // 1. Kiểm tra bảng đích
            var safeTableName = Regex.Replace(request.OutputTable, "\\W+", "");
            var checkCmd = new SqlCommand("IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
            var exists = (int)await checkCmd.ExecuteScalarAsync();
            if (exists == 1)
                throw new Exception($"Bảng '{safeTableName}' đã tồn tại.");

            // 2. Phân tích công thức: FMP = SMP + CAN
            var parts = request.Formula.Split('=');
            if (parts.Length != 2) throw new Exception("Công thức không hợp lệ.");
            var right = parts[1].Trim();

            var operands = Regex.Split(right, @"[+\-\*/]").Select(x => x.Trim()).ToList();
            var operators = Regex.Matches(right, "[+\\-\\*/]").Cast<Match>().Select(m => m.Value).ToList();

            // 3. Load các bảng liên quan
            var tables = new Dictionary<string, DataTable>();
            foreach (var tbl in request.SourceTables.Distinct())
            {
                var dt = new DataTable();
                using var cmd = new SqlCommand($"SELECT * FROM [{tbl}]", conn);
                using var adapter = new SqlDataAdapter(cmd);
                adapter.Fill(dt);
                tables[tbl] = dt;
            }

            // 4. Tạo bảng kết quả
            var resultTable = new DataTable();
            resultTable.Columns.Add("Ngày");
            resultTable.Columns.Add("Giá");

            var timeCols = tables.First().Value.Columns.Cast<DataColumn>()
                .Where(c => c.ColumnName != "Ngày" && c.ColumnName != "Giá")
                .Select(c => c.ColumnName)
                .ToList();
            foreach (var col in timeCols)
                resultTable.Columns.Add(col);

            // 5. Lấy danh sách ngày chung
            var days = tables.Values.First().AsEnumerable()
                .Select(r => r["Ngày"].ToString())
                .Where(d => !string.IsNullOrWhiteSpace(d))
                .Distinct();

            foreach (var day in days)
            {
                var newRow = resultTable.NewRow();
                newRow["Ngày"] = day;
                newRow["Giá"] = request.OutputLabel;

                foreach (var col in timeCols)
                {
                    double? result = null;
                    for (int i = 0; i < operands.Count; i++)
                    {
                        var operand = operands[i];
                        var tbl = tables.FirstOrDefault(t => t.Value.AsEnumerable().Any(r => r["Giá"].ToString()?.Trim() == operand && r["Ngày"].ToString() == day)).Value;
                        var row = tbl?.AsEnumerable().FirstOrDefault(r => r["Giá"].ToString()?.Trim() == operand && r["Ngày"].ToString() == day);

                        double.TryParse(NormalizeNumber(row?[col]?.ToString()), NumberStyles.Any, CultureInfo.InvariantCulture, out double val);

                        if (i == 0) result = val;
                        else
                        {
                            var op = operators[i - 1];
                            if (op == "+") result += val;
                            else if (op == "-") result -= val;
                            else if (op == "*") result *= val;
                            else if (op == "/") result = val == 0 ? 0 : result / val;
                        }
                    }
                    newRow[col] = result?.ToString("0.###", CultureInfo.InvariantCulture);
                }

                resultTable.Rows.Add(newRow);
            }

            // 6. Tạo bảng và insert dữ liệu
            var columnsSql = resultTable.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
            var createSql = $"CREATE TABLE [{safeTableName}] ({string.Join(", ", columnsSql)})";
            using var createCmd = new SqlCommand(createSql, conn);
            await createCmd.ExecuteNonQueryAsync();

            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = safeTableName
            };
            foreach (DataColumn col in resultTable.Columns)
            {
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }
            await bulkCopy.WriteToServerAsync(resultTable);
        } // end Task

        private string NormalizeNumber(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return "0";
            raw = raw.Trim();
            if (raw.Contains(" ") && raw.Contains("."))
                raw = raw.Replace(" ", "").Replace(",", "").Replace(".", ",");
            else if (!raw.Contains(",") && raw.Contains("."))
                raw = raw.Replace(".", ",");
            return raw.Replace(",", ".");
        }

    }
}
