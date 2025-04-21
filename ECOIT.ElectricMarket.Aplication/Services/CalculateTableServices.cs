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

        public async Task CalculateCCFDTableAsync(CalculationRequest request)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            //if (request.SourceTables.Count != 3)
            //    throw new Exception("Cần đúng 3 bảng: QC, PC và FMP.");

            var tableQC = request.SourceTables[0];
            var tablePC = request.SourceTables[1];
            var tableFMP = request.SourceTables[2];

            var safeTableName = Regex.Replace(request.OutputTable, "\\W+", "");
            var checkCmd = new SqlCommand("IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
            var exists = (int)await checkCmd.ExecuteScalarAsync();
            if (exists == 1)
                throw new Exception($"Bảng '{safeTableName}' đã tồn tại.");

            var dtQC = new DataTable();
            using (var cmd = new SqlCommand($"Select * From [{tableQC}]", conn))
            using (var adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(dtQC);
            }

            var dtPC = new DataTable();
            using (var cmd = new SqlCommand($"Select * From [{tablePC}]", conn))
            using (var adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(dtPC);
            }

            var dtFMP = new DataTable();
            using (var cmd = new SqlCommand($"Select * From [{tableFMP}]", conn))
            using (var adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(dtFMP);
            }

            var timeCols = dtFMP.Columns.Cast<DataColumn>()
                .Select(c => c.ColumnName)
                .Where(c => c != "Ngày" && c != "Giá")
                .ToList();

            var resultTable = new DataTable();
            resultTable.Columns.Add("Ngày");
            foreach (var col in timeCols)
                resultTable.Columns.Add(col);

            var days = dtQC.AsEnumerable()
                .Select(r => r["Ngày"].ToString())
                .Where(d => !string.IsNullOrWhiteSpace(d))
                .Distinct();

            foreach (var day in days)
            {
                var rowQC = dtQC.AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == day);
                var rowPC = dtPC.AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == day);
                var rowFMP = dtFMP.AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == day && r["Giá"].ToString() == "FMP");

                if (rowQC == null || rowPC == null || rowFMP == null)
                    continue;

                var newRow = resultTable.NewRow();
                newRow["Ngày"] = day;

                foreach (var col in timeCols)
                {
                    double.TryParse(NormalizeNumber(rowQC[col]?.ToString()), NumberStyles.Any, CultureInfo.InvariantCulture, out double qc);
                    double.TryParse(NormalizeNumber(rowPC[col]?.ToString()), NumberStyles.Any, CultureInfo.InvariantCulture, out double pc);
                    double.TryParse(NormalizeNumber(rowFMP[col]?.ToString()), NumberStyles.Any, CultureInfo.InvariantCulture, out double fmp);

                    var ccfd = Math.Round(qc * (pc - fmp) / 1000.6, 2);
                    newRow[col] = ccfd.ToString("0.00", CultureInfo.InvariantCulture);

                }

                resultTable.Rows.Add(newRow);
            }

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
            raw = raw.Replace(" ", "").Trim();

            // Nếu số có 2 dấu phân cách → xử lý theo kiểu Việt
            if (Regex.IsMatch(raw, @"^\d{1,3}(\.\d{3})+,\d+$"))
            {
                // Ví dụ: 1.234.567,89 → 1234567.89
                raw = raw.Replace(".", "").Replace(",", ".");
            }
            else if (Regex.IsMatch(raw, @"^\d{1,3}(,\d{3})+\.\d+$"))
            {
                // Ví dụ: 1,234,567.89 (kiểu US) → 1234567.89
                raw = raw.Replace(",", "");
            }
            else
            {
                raw = raw.Replace(",", ".");
            }

            return raw;
        }

    }
}
