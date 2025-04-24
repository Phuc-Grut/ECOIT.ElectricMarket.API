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

            var tableQC = request.SourceTables[0];
            var tablePC = request.SourceTables[1];
            var tableFMP = request.SourceTables[2];

            if (!tableQC.StartsWith("QC") || !tablePC.StartsWith("PC") || tableFMP != "FMP")
                throw new Exception("Vui lòng chọn đúng thứ tự: QC → PC → FMP");

            var safeTableName = Regex.Replace(request.OutputTable, "\\W+", "");
            var checkCmd = new SqlCommand("IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
            var exists = (int)await checkCmd.ExecuteScalarAsync();
            if (exists == 1)
                throw new Exception($"Bảng '{safeTableName}' đã tồn tại.");

            // Load dữ liệu từ các bảng nguồn
            var dtQC = await LoadTableAsync(conn, tableQC);
            var dtPC = await LoadTableAsync(conn, tablePC);
            var dtFMP = await LoadTableAsync(conn, tableFMP);

            var timeCols = dtFMP.Columns.Cast<DataColumn>()
                .Select(c => c.ColumnName)
                .Where(c => c != "Ngày" && c != "Giá" && c != "Tổng")
                .ToList();

            var resultTable = new DataTable();
            resultTable.Columns.Add("Ngày", typeof(string));
            foreach (var col in timeCols)
                resultTable.Columns.Add(col, typeof(double));

            resultTable.Columns.Add("Tổng", typeof(double));

            var days = dtQC.AsEnumerable()
                .Select(r => r["Ngày"]?.ToString())
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

                double rowSum = 0;
                    
                foreach (var col in timeCols)
                {
                    double.TryParse(NormalizeNumber(rowQC[col]?.ToString()), NumberStyles.Any, CultureInfo.InvariantCulture, out double qc);
                    double.TryParse(NormalizeNumber(rowPC[col]?.ToString()), NumberStyles.Any, CultureInfo.InvariantCulture, out double pc);
                    double.TryParse(NormalizeNumber(rowFMP[col]?.ToString()), NumberStyles.Any, CultureInfo.InvariantCulture, out double fmp);

                    var ccfd = Math.Round(qc * (pc - fmp) / 1000, 6);

                    newRow[col] = ccfd;
                    rowSum += ccfd;
                }
                newRow["Tổng"] = rowSum;
                resultTable.Rows.Add(newRow);
            }

            // Tạo bảng trong SQL
            var columnDefs = resultTable.Columns.Cast<DataColumn>()
                .Select(c =>
                {
                    return $"[{c.ColumnName}] {(c.DataType == typeof(double) ? "DECIMAL(18,6)" : "NVARCHAR(MAX)")}";
                });

            var createSql = $"CREATE TABLE [{safeTableName}] ({string.Join(", ", columnDefs)})";
            using var createCmd = new SqlCommand(createSql, conn);
            await createCmd.ExecuteNonQueryAsync();

            // Ghi dữ liệu
            using var bulkCopy = new SqlBulkCopy(conn) { DestinationTableName = safeTableName };
            foreach (DataColumn col in resultTable.Columns)
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            await bulkCopy.WriteToServerAsync(resultTable);
        }

        private async Task<DataTable> LoadTableAsync(SqlConnection conn, string tableName)
        {
            var dt = new DataTable();
            using var cmd = new SqlCommand($"SELECT * FROM [{tableName}]", conn);
            using var adapter = new SqlDataAdapter(cmd);
            adapter.Fill(dt);
            return dt;
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

        //public async Task CalculateSanLuongHopDongAsync(string outputTable)
        //{
        //    var sourceTables = new[] { "QC_PM1", "QC_PM4", "QC_TB1",  "QC_VT44MR", "QC_DH3_MR", };
        //    var safeTableName = Regex.Replace(outputTable, "\\W+", "");

        //    using var conn = new SqlConnection(_connectionString);
        //    await conn.OpenAsync();

        //    // 1. Kiểm tra bảng đích đã tồn tại chưa
        //    var checkCmd = new SqlCommand("IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
        //    checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
        //    var exists = (int)await checkCmd.ExecuteScalarAsync();
        //    if (exists == 1)
        //        throw new Exception($"Bảng '{safeTableName}' đã tồn tại.");

        //    // 2. Tải dữ liệu từ các bảng nguồn
        //    var data = new Dictionary<string, DataTable>();
        //    foreach (var tbl in sourceTables)
        //    {
        //        var dt = new DataTable();
        //        using var cmd = new SqlCommand($"SELECT * FROM [{tbl}]", conn);
        //        using var adapter = new SqlDataAdapter(cmd);
        //        adapter.Fill(dt);
        //        data[tbl] = dt;
        //    }

        //    // 3. Tạo bảng kết quả
        //    var resultTable = new DataTable();
        //    resultTable.Columns.Add("Ngày", typeof(string));
        //    resultTable.Columns.Add("PM1", typeof(string));
        //    resultTable.Columns.Add("PM4", typeof(string));
        //    resultTable.Columns.Add("TB1", typeof(string));
        //    resultTable.Columns.Add("VT44MR", typeof(string));
        //    resultTable.Columns.Add("DH3_MR", typeof(string));
        //    resultTable.Columns.Add("SanLuongHopDong", typeof(string));

        //    var allDays = data.SelectMany(d => d.Value.AsEnumerable().Select(r => r["Ngày"].ToString())).Distinct();

        //    foreach (var day in allDays)
        //    {
        //        var newRow = resultTable.NewRow();
        //        newRow["Ngày"] = day;

        //        double pm1 = 0, pm4 = 0, tb1 = 0, dh3 = 0, vt = 0;

        //        foreach (var kv in data)
        //        {
        //            var row = kv.Value.AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == day);
        //            double.TryParse(row?["Tổng"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double val);
        //            double valX1000 = val * 1000;

        //            if (kv.Key.Contains("PM1", StringComparison.OrdinalIgnoreCase)) pm1 = valX1000;
        //            if (kv.Key.Contains("PM4", StringComparison.OrdinalIgnoreCase)) pm4 = valX1000;
        //            if (kv.Key.Contains("TB1", StringComparison.OrdinalIgnoreCase)) tb1 = valX1000;
        //            if (kv.Key.Contains("DH3", StringComparison.OrdinalIgnoreCase)) dh3 = valX1000;
        //            if (kv.Key.Contains("VT", StringComparison.OrdinalIgnoreCase)) vt = valX1000;
        //        }

        //        newRow["PM1"] = pm1.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["PM4"] = pm4.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["TB1"] = tb1.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["VT44MR"] = vt.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["DH3_MR"] = dh3.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["SanLuongHopDong"] = (pm1 + pm4 + tb1 + dh3 + vt).ToString("#,0", CultureInfo.InvariantCulture);

        //        resultTable.Rows.Add(newRow);
        //    }

        //    var totalRow = resultTable.NewRow();
        //    totalRow["Ngày"] = "Tổng";

        //    double totalPM1 = 0, totalPM4 = 0, totalTB1 = 0, totalDH3 = 0, totalVT = 0, totalSLHD = 0;

        //    foreach (DataRow row in resultTable.Rows)
        //    {
        //        double.TryParse(row["PM1"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vPM1);
        //        double.TryParse(row["PM4"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vPM4);
        //        double.TryParse(row["TB1"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vTB1);

        //        double.TryParse(row["VT44MR"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vVT);
        //        double.TryParse(row["DH3_MR"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vDH3);
        //        double.TryParse(row["SanLuongHopDong"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vSLHD);

        //        totalPM1 += vPM1;
        //        totalPM4 += vPM4;
        //        totalTB1 += vTB1;
        //        totalVT += vVT;
        //        totalDH3 += vDH3;
        //        totalSLHD += vSLHD;
        //    }

        //    totalRow["PM1"] = totalPM1.ToString("#,0", CultureInfo.InvariantCulture);
        //    totalRow["PM4"] = totalPM4.ToString("#,0", CultureInfo.InvariantCulture);
        //    totalRow["TB1"] = totalTB1.ToString("#,0", CultureInfo.InvariantCulture);

        //    totalRow["VT44MR"] = totalVT.ToString("#,0", CultureInfo.InvariantCulture);
        //    totalRow["DH3_MR"] = totalDH3.ToString("#,0", CultureInfo.InvariantCulture);
        //    totalRow["SanLuongHopDong"] = totalSLHD.ToString("#,0", CultureInfo.InvariantCulture);

        //    resultTable.Rows.Add(totalRow);


        //    // 4. Tạo bảng trong SQL Server
        //    var columnDefs = resultTable.Columns.Cast<DataColumn>()
        //        .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
        //    var createSql = $"CREATE TABLE [{safeTableName}] ({string.Join(", ", columnDefs)})";
        //    using var createCmd = new SqlCommand(createSql, conn);
        //    await createCmd.ExecuteNonQueryAsync();

        //    // 5. Ghi dữ liệu vào bảng
        //    using var bulkCopy = new SqlBulkCopy(conn) { DestinationTableName = safeTableName };
        //    foreach (DataColumn col in resultTable.Columns)
        //        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);

        //    await bulkCopy.WriteToServerAsync(resultTable);
        //}

        //public async Task CalculateChiPhiAsync(string outputTable)
        //{
        //    var sourceTables = new[] { "CCFD_PM1", "CCFD_PM4", "CCFD_TB1", "CCFD_VT44MR", "CCFD_DH3_MR", };
        //    var safeTableName = Regex.Replace(outputTable, "\\W+", "");

        //    using var conn = new SqlConnection(_connectionString);
        //    await conn.OpenAsync();

        //    // 1. Kiểm tra bảng đích đã tồn tại chưa
        //    var checkCmd = new SqlCommand("IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
        //    checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
        //    var exists = (int)await checkCmd.ExecuteScalarAsync();
        //    if (exists == 1)
        //        throw new Exception($"Bảng '{safeTableName}' đã tồn tại.");

        //    // 2. Tải dữ liệu từ các bảng nguồn
        //    var data = new Dictionary<string, DataTable>();
        //    foreach (var tbl in sourceTables)
        //    {
        //        var dt = new DataTable();
        //        using var cmd = new SqlCommand($"SELECT * FROM [{tbl}]", conn);
        //        using var adapter = new SqlDataAdapter(cmd);
        //        adapter.Fill(dt);
        //        data[tbl] = dt;
        //    }

        //    // 3. Tạo bảng kết quả
        //    var resultTable = new DataTable();
        //    resultTable.Columns.Add("Ngày", typeof(string));
        //    resultTable.Columns.Add("PM1", typeof(string));
        //    resultTable.Columns.Add("PM4", typeof(string));
        //    resultTable.Columns.Add("TB1", typeof(string));
        //    resultTable.Columns.Add("VT44MR", typeof(string));
        //    resultTable.Columns.Add("DH3_MR", typeof(string));
        //    resultTable.Columns.Add("ChiPhi", typeof(string));

        //    var allDays = data.SelectMany(d => d.Value.AsEnumerable().Select(r => r["Ngày"].ToString())).Distinct();

        //    foreach (var day in allDays)
        //    {
        //        var newRow = resultTable.NewRow();
        //        newRow["Ngày"] = day;

        //        double pm1 = 0, pm4 = 0, tb1 = 0, dh3 = 0, vt = 0;

        //        foreach (var kv in data)
        //        {
        //            var row = kv.Value.AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == day);
        //            double.TryParse(row?["Tổng"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double val);
        //            double valX10Mu6 = val * Math.Pow(10, 6);

        //            if (kv.Key.Contains("PM1", StringComparison.OrdinalIgnoreCase)) pm1 = valX10Mu6;
        //            if (kv.Key.Contains("PM4", StringComparison.OrdinalIgnoreCase)) pm4 = valX10Mu6;
        //            if (kv.Key.Contains("TB1", StringComparison.OrdinalIgnoreCase)) tb1 = valX10Mu6;
        //            if (kv.Key.Contains("DH3", StringComparison.OrdinalIgnoreCase)) dh3 = valX10Mu6;
        //            if (kv.Key.Contains("VT", StringComparison.OrdinalIgnoreCase)) vt = valX10Mu6;
        //        }

        //        newRow["PM1"] = pm1.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["PM4"] = pm4.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["TB1"] = tb1.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["VT44MR"] = vt.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["DH3_MR"] = dh3.ToString("#,0", CultureInfo.InvariantCulture);
        //        newRow["ChiPhi"] = (pm1 + pm4 + tb1 + dh3 + vt).ToString("#,0", CultureInfo.InvariantCulture);

        //        resultTable.Rows.Add(newRow);
        //    }

        //    var totalRow = resultTable.NewRow();
        //    totalRow["Ngày"] = "Tổng";

        //    double totalPM1 = 0, totalPM4 = 0, totalTB1 = 0, totalDH3 = 0, totalVT = 0, totalSLHD = 0;

        //    foreach (DataRow row in resultTable.Rows)
        //    {
        //        double.TryParse(row["PM1"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vPM1);
        //        double.TryParse(row["PM4"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vPM4);
        //        double.TryParse(row["TB1"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vTB1);

        //        double.TryParse(row["VT44MR"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vVT);
        //        double.TryParse(row["DH3_MR"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vDH3);
        //        double.TryParse(row["ChiPhi"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double vSLHD);

        //        totalPM1 += vPM1;
        //        totalPM4 += vPM4;
        //        totalTB1 += vTB1;
        //        totalVT += vVT;
        //        totalDH3 += vDH3;
        //        totalSLHD += vSLHD;
        //    }

        //    totalRow["PM1"] = totalPM1.ToString("#,0", CultureInfo.InvariantCulture);
        //    totalRow["PM4"] = totalPM4.ToString("#,0", CultureInfo.InvariantCulture);
        //    totalRow["TB1"] = totalTB1.ToString("#,0", CultureInfo.InvariantCulture);

        //    totalRow["VT44MR"] = totalVT.ToString("#,0", CultureInfo.InvariantCulture);
        //    totalRow["DH3_MR"] = totalDH3.ToString("#,0", CultureInfo.InvariantCulture);
        //    totalRow["ChiPhi"] = totalSLHD.ToString("#,0", CultureInfo.InvariantCulture);

        //    resultTable.Rows.Add(totalRow);


        //    // 4. Tạo bảng trong SQL Server
        //    var columnDefs = resultTable.Columns.Cast<DataColumn>()
        //        .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
        //    var createSql = $"CREATE TABLE [{safeTableName}] ({string.Join(", ", columnDefs)})";
        //    using var createCmd = new SqlCommand(createSql, conn);
        //    await createCmd.ExecuteNonQueryAsync();

        //    // 5. Ghi dữ liệu vào bảng
        //    using var bulkCopy = new SqlBulkCopy(conn) { DestinationTableName = safeTableName };
        //    foreach (DataColumn col in resultTable.Columns)
        //        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);

        //    await bulkCopy.WriteToServerAsync(resultTable);
        //}

        public async Task CalculateTongHopAsync()
        {
            var sanLuongTables = new[] { "QC_PM1", "QC_PM4", "QC_TB1", "QC_VT44MR", "QC_DH3_MR" };
            var chiPhiTables = new[] { "CCFD_PM1", "CCFD_PM4", "CCFD_TB1", "CCFD_VT44MR", "CCFD_DH3_MR" };

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            async Task<Dictionary<string, DataTable>> LoadData(string[] tables)
            {
                var data = new Dictionary<string, DataTable>();
                foreach (var tbl in tables)
                {
                    var dt = new DataTable();
                    using var cmd = new SqlCommand($"SELECT * FROM [{tbl}]", conn);
                    using var adapter = new SqlDataAdapter(cmd);
                    adapter.Fill(dt);
                    data[tbl] = dt;
                }
                return data;
            }

            var dataSL = await LoadData(sanLuongTables);
            var dataCP = await LoadData(chiPhiTables);

            var allDays = dataSL
                .SelectMany(d => d.Value.AsEnumerable().Select(r => r["Ngày"].ToString()))
                .Concat(dataCP.SelectMany(d => d.Value.AsEnumerable().Select(r => r["Ngày"].ToString())))
                .Distinct();

            var resultTable = new DataTable();
            resultTable.Columns.Add("Ngày", typeof(string));

            var prefixs = new[] { "PM1", "PM4", "TB1", "VT44MR", "DH3_MR" };
            foreach (var p in prefixs)
            {
                resultTable.Columns.Add(p + "_SL", typeof(string));
                resultTable.Columns.Add(p + "_CP", typeof(string));
            }
            resultTable.Columns.Add("Tong_SL", typeof(string));
            resultTable.Columns.Add("Tong_CP", typeof(string));

            foreach (var day in allDays)
            {
                var row = resultTable.NewRow();
                row["Ngày"] = day;

                double tongSL = 0, tongCP = 0;

                foreach (var p in prefixs)
                {
                    double sl = 0, cp = 0;

                    var keySL = dataSL.Keys.FirstOrDefault(k => k.Contains(p, StringComparison.OrdinalIgnoreCase));
                    var keyCP = dataCP.Keys.FirstOrDefault(k => k.Contains(p, StringComparison.OrdinalIgnoreCase));

                    if (keySL != null)
                    {
                        var r = dataSL[keySL].AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == day);
                        double.TryParse(r?["Tổng"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out sl);
                        sl *= 1000;
                    }

                    if (keyCP != null)
                    {
                        var r = dataCP[keyCP].AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == day);
                        double.TryParse(r?["Tổng"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out cp);
                        cp *= 1_000_000;
                    }

                    row[p + "_SL"] = sl.ToString("#,0", CultureInfo.InvariantCulture);
                    row[p + "_CP"] = cp.ToString("#,0", CultureInfo.InvariantCulture);
                    tongSL += sl;
                    tongCP += cp;
                }

                row["Tong_SL"] = tongSL.ToString("#,0", CultureInfo.InvariantCulture);
                row["Tong_CP"] = tongCP.ToString("#,0", CultureInfo.InvariantCulture);

                resultTable.Rows.Add(row);
            }

            // Tính dòng tổng
            var totalRow = resultTable.NewRow();
            totalRow["Ngày"] = "Tổng";

            foreach (DataColumn col in resultTable.Columns)
            {
                if (col.ColumnName != "Ngày")
                {
                    double sum = 0;
                    foreach (DataRow row in resultTable.Rows)
                    {
                        if (row["Ngày"].ToString() == "Tổng") continue;
                        double.TryParse(row[col.ColumnName]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out double val);
                        sum += val;
                    }
                    totalRow[col.ColumnName] = sum.ToString("#,0", CultureInfo.InvariantCulture);
                }
            }
            resultTable.Rows.Add(totalRow);

            // Sắp xếp lại đúng thứ tự cột mong muốn
            var orderedColumns = new[]
            {
                "Ngày",
                "PM1_SL", "PM4_SL", "TB1_SL", "VT44MR_SL", "DH3_MR_SL", "Tong_SL",
                "PM1_CP", "PM4_CP", "TB1_CP", "VT44MR_CP", "DH3_MR_CP", "Tong_CP"
            };

            var orderedTable = new DataTable();
            foreach (var colName in orderedColumns)
                orderedTable.Columns.Add(colName, typeof(string));

            foreach (DataRow oldRow in resultTable.Rows)
            {
                var newRow = orderedTable.NewRow();
                foreach (var colName in orderedColumns)
                    newRow[colName] = oldRow[colName];
                orderedTable.Rows.Add(newRow);
            }

            // Ghi vào SQL
            using (var dropCmd = new SqlCommand("IF OBJECT_ID('TongHop', 'U') IS NOT NULL DROP TABLE TongHop", conn))
                await dropCmd.ExecuteNonQueryAsync();

            var columnDefs = orderedTable.Columns.Cast<DataColumn>().Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
            var createSql = $"CREATE TABLE TongHop ({string.Join(", ", columnDefs)})";
            using (var createCmd = new SqlCommand(createSql, conn))
                await createCmd.ExecuteNonQueryAsync();

            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = "TongHop" };
            foreach (DataColumn col in orderedTable.Columns)
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

            await bulk.WriteToServerAsync(orderedTable);
        }


    }
}
