using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using ECOIT.ElectricMarket.Application.Interface;
using ECOIT.ElectricMarket.Application.DTO;
using System.Text.RegularExpressions;

namespace ECOIT.ElectricMarket.Application.Services
{
    public class File4Services : IFile4Services
    {
        private readonly string _connectionString;

        public File4Services(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection");
        }

        public async Task CalculateAndInsertQM1_24ChukyAsync()
        {
            using var conn = new SqlConnection(_connectionString);
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
                        newRow[$"{i}h"] = i.ToString();

                    newRow["Tổng"] = "Tổng";
                    dtResult.Rows.Add(newRow);
                    continue;
                }

                decimal total = 0;
                for (int i = 0; i < 24; i++)
                {
                    int colIndex1 = 2 + (i * 2);
                    int colIndex2 = 3 + (i * 2);

                    decimal.TryParse(row[colIndex1]?.ToString(), out decimal d1);
                    decimal.TryParse(row[colIndex2]?.ToString(), out decimal d2);

                    decimal sum = d1 + d2;
                    total += sum;

                    newRow[$"{i + 1}h"] = Math.Round(sum).ToString("N0", CultureInfo.InvariantCulture);
                }

                newRow["Tổng"] = Math.Round(total).ToString("N0", CultureInfo.InvariantCulture);
                dtResult.Rows.Add(newRow);
            }

            // Tạo bảng nếu chưa có
            var createTableSql = GenerateCreateTableSql("QM1_24Chuky", dtResult);
            using (var cmdCreate = new SqlCommand(createTableSql, conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            using (var truncateCmd = new SqlCommand("TRUNCATE TABLE dbo.QM1_24Chuky", conn))
            {
                await truncateCmd.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = "dbo.QM1_24Chuky";
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }
                await bulkCopy.WriteToServerAsync(dtResult);
            }
        }

        public async Task<string> CalculateAndInsertQM1_48ChukyAsync()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dtSchema = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT TOP 1 * FROM QM1_PhuTaiENV", conn))
            {
                adapter.FillSchema(dtSchema, SchemaType.Source);
            }

            var dtSource = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT * FROM QM1_PhuTaiENV", conn))
            {
                adapter.Fill(dtSource);
            }

            var dtResult = dtSource.Clone();

            var x1Dict = new Dictionary<string, decimal>();
            using (var cmd = new SqlCommand("SELECT Gio, X1 FROM X1", conn))
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    x1Dict[reader.GetString(0)] = reader.GetDecimal(1);
                }
            }

            foreach (DataRow row in dtSource.Rows)
            {
                var newRow = dtResult.NewRow();

                if (row["Chukì"]?.ToString() == "Ngày" || row["Col2"]?.ToString() == "Thứ")
                {
                    foreach (DataColumn col in dtSource.Columns)
                    {
                        newRow[col.ColumnName] = row[col.ColumnName];
                    }

                    dtResult.Rows.Add(newRow);
                    continue;
                }

                decimal rowSum = 0;

                foreach (DataColumn col in dtSource.Columns)
                {
                    string colName = col.ColumnName;

                    if (colName == "Chukì" || colName == "Col2")
                    {
                        newRow[colName] = row[colName]?.ToString();
                    }
                    else if (colName == "Tổng")
                    {
                        continue;
                    }
                    else
                    {
                        var cell = row[colName]?.ToString();
                        if (decimal.TryParse(cell, out decimal value) &&
                            x1Dict.TryGetValue(colName, out decimal x1Value))
                        {
                            var result = Math.Round(value * x1Value);
                            rowSum += result;
                            newRow[colName] = result.ToString("N0", CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            newRow[colName] = cell;
                        }
                    }
                }

                newRow["Tổng"] = rowSum.ToString("N0", CultureInfo.InvariantCulture);
                dtResult.Rows.Add(newRow);
            }

            string createTableSql = GenerateCreateTableSql("QM1_48Chuky", dtResult);
            using (var createCmd = new SqlCommand(createTableSql, conn))
            {
                await createCmd.ExecuteNonQueryAsync();
            }

            using (var truncateCmd = new SqlCommand("TRUNCATE TABLE dbo.QM1_48Chuky", conn))
            {
                await truncateCmd.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = "dbo.QM1_48Chuky";
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }
                await bulkCopy.WriteToServerAsync(dtResult);
            }

            return "Tính QM1 48 chu kỳ thành công";
        }

        public async Task CalculateAndInsertX2ProvinceAsync(string province)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dtSchema = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT TOP 1 * FROM QM1_48Chuky", conn))
            {
                adapter.FillSchema(dtSchema, SchemaType.Source);
            }
 
            var dtResult = new DataTable();
            dtResult.Columns.Add("Chukì", typeof(DateTime));
            dtResult.Columns.Add("Col2", typeof(object));

            foreach (DataColumn col in dtSchema.Columns)
            {
                if (col.ColumnName != "Chukì" && col.ColumnName != "Col2")
                    dtResult.Columns.Add(col.ColumnName, typeof(decimal));
            }

            if (!dtResult.Columns.Contains("Tổng"))
                dtResult.Columns.Add("Tổng", typeof(string));

            var dtSource = new DataTable();
            var provinceSafe = province.Replace("'", "''");
            var sqlX2 = $"SELECT * FROM X2 WHERE [Đơnvị] = N'{provinceSafe}'";
            using (var adapter = new SqlDataAdapter(sqlX2, conn))
            {
                adapter.Fill(dtSource);
            }

            var dtNgayThu = new DataTable();
            using (var ngayAdapter = new SqlDataAdapter("SELECT Chukì, Col2 FROM QM1_48Chuky WHERE Chukì <> 'Ngày'", conn))
            {
                ngayAdapter.Fill(dtNgayThu);
            }

            int validRowIndex = 0;

            foreach (DataRow row in dtSource.Rows)
            {
                if (validRowIndex >= dtNgayThu.Rows.Count)
                    break;

                var chukiStr = dtNgayThu.Rows[validRowIndex]["Chukì"]?.ToString()?.Trim();

                if (!DateTime.TryParseExact(chukiStr, "d/M/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var chukiDate))
                {
                    validRowIndex++;
                    continue;
                }

                var newRow = dtResult.NewRow();
                newRow["Chukì"] = chukiDate;
                newRow["Col2"] = dtNgayThu.Rows[validRowIndex]["Col2"];

                decimal total = 0;

                foreach (DataColumn col in dtResult.Columns)
                {
                    var colName = col.ColumnName;
                    if (colName == "Chukì" || colName == "Col2" || colName == "Tổng")
                        continue;

                    string srcCol = dtSource.Columns
                        .Cast<DataColumn>()
                        .FirstOrDefault(c => c.ColumnName.EndsWith("_" + colName))?.ColumnName;

                    if (!string.IsNullOrEmpty(srcCol) && dtSource.Columns.Contains(srcCol))
                    {
                        var val = row[srcCol];
                        if (decimal.TryParse(val?.ToString(), out decimal parsedVal))
                        {
                            newRow[colName] = parsedVal;
                            total += parsedVal;
                        }
                        else
                        {
                            newRow[colName] = 0;
                        }
                    }
                }

                newRow["Tổng"] = Math.Round(total, 2).ToString("N2", CultureInfo.InvariantCulture);
                dtResult.Rows.Add(newRow);
                validRowIndex++;
            }

            if (dtResult.Rows.Count > 0 && dtResult.Columns["Chukì"] != null)
            {
                dtResult = dtResult.AsEnumerable()
                    .OrderBy(r => r.Field<DateTime?>("Chukì"))
                    .CopyToDataTable();
            }

            string tableName = "X2_" + RemoveDiacritics(province).Replace(" ", "");

            var createTableSql = GenerateCreateTableSql(tableName, dtResult);
            using (var cmdCreate = new SqlCommand(createTableSql, conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            using (var cmdTruncate = new SqlCommand($"TRUNCATE TABLE [{tableName}]", conn))
            {
                await cmdTruncate.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = tableName;
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dtResult);
            }
        }


        public static string RemoveDiacritics(string text)
        {
            string normalized = text.Normalize(NormalizationForm.FormD);
            var sb = new StringBuilder();
            foreach (var c in normalized)
            {
                var unicodeCategory = CharUnicodeInfo.GetUnicodeCategory(c);
                if (unicodeCategory != UnicodeCategory.NonSpacingMark)
                {
                    sb.Append(c);
                }
            }
            return sb.ToString().Normalize(NormalizationForm.FormC);
        }

        private string GenerateCreateTableSql(string tableName, DataTable schema)
        {
            var columns = new List<string>();

            foreach (DataColumn col in schema.Columns)
            {
                string sqlType;

                if (col.ColumnName == "Chukì")
                {
                    sqlType = "NVARCHAR(50)";
                }
                else
                {
                    sqlType = "NVARCHAR(MAX)";
                    if (col.DataType == typeof(int)) sqlType = "INT";
                    else if (col.DataType == typeof(double)) sqlType = "FLOAT";
                }

                columns.Add($"[{col.ColumnName}] {sqlType}");
            }

            return $@"
            IF OBJECT_ID('dbo.{tableName}', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{tableName} (
                    {string.Join(",", columns)}
                )
            END";
        }


        public async Task CalculateProvinceAsync(string province, string tableName)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dtSource = new DataTable();
            using (var adapter = new SqlDataAdapter($"SELECT * FROM [{tableName}]", conn))
            {
                adapter.Fill(dtSource);
            }

            var dtResult = dtSource.Clone();

            if (!dtResult.Columns.Contains("Tổng"))
                dtResult.Columns.Add("Tổng");

            foreach (DataRow row in dtSource.Rows)
            {
                var newRow = dtResult.NewRow();
                decimal total = 0;

                foreach (DataColumn col in dtSource.Columns)
                {
                    var colName = col.ColumnName;

                    if (colName == "Chukì" || colName == "Col2" || colName == "Tổng")
                    {
                        newRow[colName] = row[colName];
                        continue;
                    }

                    var value = row[colName];
                    if (decimal.TryParse(value?.ToString(), out decimal val))
                    {
                        decimal divided = val / 100m;
                        newRow[colName] = Math.Round(divided, 5);
                        total += divided;
                    }
                    else
                    {
                        newRow[colName] = 0;
                    }
                }

                newRow["Tổng"] = Math.Round(total, 5).ToString("N5", CultureInfo.InvariantCulture);
                dtResult.Rows.Add(newRow);
            }

            string newTableName = RemoveDiacritics(province).Replace(" ", "");

            var createSql = GenerateCreateTableSql(newTableName, dtResult);
            using (var cmdCreate = new SqlCommand(createSql, conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            using (var cmdTruncate = new SqlCommand($"TRUNCATE TABLE [{newTableName}]", conn))
            {
                await cmdTruncate.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = newTableName;
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dtResult);
            }
        }

        public async Task CalculateQM2ProvinceAsync(string province, string tableName)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dtQM1 = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT * FROM QM1_PhuTaiENV", conn))
            {
                adapter.Fill(dtQM1);
            }
            if (dtQM1.Rows.Count > 0)
            {
                dtQM1.Rows[0].Delete();
                dtQM1.AcceptChanges();
            }

            var dtX2 = new DataTable();
            using (var adapter = new SqlDataAdapter($"SELECT * FROM [{tableName}]", conn))
            {
                adapter.Fill(dtX2);
            }

            var dtResult = dtX2.Clone();

            if (!dtResult.Columns.Contains("Tổng"))
                dtResult.Columns.Add("Tổng");

            for (int i = 0; i < dtQM1.Rows.Count; i++)
            {
                var rowQM1 = dtQM1.Rows[i];
                var rowX2 = i < dtX2.Rows.Count ? dtX2.Rows[i] : null;

                var newRow = dtResult.NewRow();
                decimal total = 0;

                foreach (DataColumn col in dtResult.Columns)
                {
                    var colName = col.ColumnName;

                    if (colName == "Chukì" || colName == "Col2")
                    {
                        if (rowX2 != null && dtX2.Columns.Contains(colName))
                            newRow[colName] = rowX2[colName];
                        else
                            newRow[colName] = DBNull.Value;
                        continue;
                    }

                    if (colName == "Tổng")
                        continue;

                    if (decimal.TryParse(rowQM1[colName]?.ToString(), out var val1) &&
                        rowX2 != null && decimal.TryParse(rowX2[colName]?.ToString(), out var val2))
                    {
                        var multiplied = Math.Round(val1 * val2, 5);
                        newRow[colName] = Math.Round(multiplied, 0).ToString("N0", CultureInfo.InvariantCulture);

                        total += multiplied;
                    }
                    else
                    {
                        newRow[colName] = 0;
                    }
                }
                newRow["Tổng"] = Math.Round(total, 0).ToString("N0", CultureInfo.InvariantCulture);
                dtResult.Rows.Add(newRow);
            }

            string newTableName = "QM2_" + RemoveDiacritics(province).Replace(" ", "");

            var createTableSql = GenerateCreateTableSql(newTableName, dtResult);
            using (var cmdCreate = new SqlCommand(createTableSql, conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            using (var cmdTruncate = new SqlCommand($"TRUNCATE TABLE [{newTableName}]", conn))
            {
                await cmdTruncate.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = newTableName;
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dtResult);
            }
        }

        public async Task CalculateQM2_24ChukyAsync(string province, string tableName)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dtSource = new DataTable();
            using (var adapter = new SqlDataAdapter($"SELECT * FROM  [{tableName}]", conn))
            {
                adapter.Fill(dtSource);
            }

            var dtResult = new DataTable();
            dtResult.Columns.Add("Chukì", typeof(string));

            dtResult.Columns.Add("Col2");
            for (int i = 1; i <= 24; i++)
            {
                dtResult.Columns.Add($"{i}h");
            }
            dtResult.Columns.Add("Tổng");

            foreach (DataRow row in dtSource.Rows)
            {
                var newRow = dtResult.NewRow();

                newRow["Chukì"] = NormalizeDate(row["Chukì"]);

                newRow["Col2"] = row["Col2"];

                if (row["Chukì"] == "Ngày" || row["Col2"] == "Thứ")
                {
                    for (int i = 1; i <= 24; i++)
                        newRow[$"{i}h"] = i.ToString();

                    newRow["Tổng"] = "Tổng";
                    dtResult.Rows.Add(newRow);
                    continue;
                }

                decimal total = 0;
                for (int i = 0; i < 24; i++)
                {
                    int colIndex1 = 2 + (i * 2);
                    int colIndex2 = 3 + (i * 2);

                    decimal.TryParse(row[colIndex1]?.ToString(), out decimal d1);
                    decimal.TryParse(row[colIndex2]?.ToString(), out decimal d2);

                    decimal sum = d1 + d2;
                    total += sum;

                    newRow[$"{i + 1}h"] = Math.Round(sum).ToString("N0", CultureInfo.InvariantCulture);
                }

                newRow["Tổng"] = Math.Round(total).ToString("N0", CultureInfo.InvariantCulture);
                dtResult.Rows.Add(newRow);
            }
            var resultTableName = $"QM2_{province}_24Chuky";


            // tạo bảng
            var createTableSql = GenerateCreateTableSql(resultTableName, dtResult);
            using (var cmdCreate = new SqlCommand(createTableSql, conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            using (var truncateCmd = new SqlCommand($"TRUNCATE TABLE dbo.[{resultTableName}]", conn))
            {
                await truncateCmd.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = $"dbo.[{resultTableName}]";
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }
                await bulkCopy.WriteToServerAsync(dtResult);
            }
        }

        public async Task CalculateQMTongHop(CalculationRequest request)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var safeTableName = Regex.Replace(request.OutputTable, "\\W+", "");
            var checkCmd = new SqlCommand("IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
            var exists = (int)await checkCmd.ExecuteScalarAsync();
            if (exists == 1)
                throw new Exception($"Bảng '{safeTableName}' đã tồn tại.");

            var parts = request.Formula.Split('=');
            if (parts.Length != 2) throw new Exception("Công thức không hợp lệ.");
            var right = parts[1].Trim();

            var operands = Regex.Split(right, @"[+\-\*/]").Select(x => x.Trim()).ToList();
            var operators = Regex.Matches(right, "[+\\-\\*/]").Cast<Match>().Select(m => m.Value).ToList();

            var tables = new Dictionary<string, DataTable>();
            foreach (var tbl in request.SourceTables.Distinct())
            {
                var dt = new DataTable();
                using var cmd = new SqlCommand($"SELECT * FROM [{tbl}]", conn);
                using var adapter = new SqlDataAdapter(cmd);
                adapter.Fill(dt);
                tables[tbl] = dt;
            }

            // ===== TẠO KẾT QUẢ =====
            var resultTable = new DataTable("KQ_" + safeTableName);
            resultTable.Columns.Clear();

            // Thêm cột "Ngày"
            if (!resultTable.Columns.Contains("Ngày"))
                resultTable.Columns.Add("Ngày");

            // Lấy các cột giờ từ bảng đầu tiên
            var timeCols = tables.Values.First().Columns.Cast<DataColumn>()
                .Where(c => c.ColumnName != "Chukì" && c.ColumnName != "Col2" && c.ColumnName != "Tổng" && c.ColumnName != "Ngày")
                .Select(c => c.ColumnName)
                .ToList();

            // Thêm các cột giờ
            foreach (var col in timeCols)
            {
                if (!resultTable.Columns.Contains(col))
                    resultTable.Columns.Add(col);
            }

            // Thêm cột Tổng
            if (!resultTable.Columns.Contains("Tổng"))
                resultTable.Columns.Add("Tổng");

            // Lấy ngày hợp lệ
            var days = tables.Values.First().AsEnumerable()
                .Where(r => !IsHeaderRow(r))
                .Select(r => NormalizeDate(
                    r.Table.Columns.Contains("Chukì") ? r["Chukì"]?.ToString() : null
                ))
                .Where(d => !string.IsNullOrWhiteSpace(d))
                .Distinct();

            foreach (var day in days)
            {
                var newRow = resultTable.NewRow();
                newRow["Ngày"] = day;
                double sum = 0;

                foreach (var col in timeCols)
                {
                    double? result = null;

                    for (int i = 0; i < operands.Count; i++)
                    {
                        var operand = operands[i];
                        if (!tables.ContainsKey(operand)) continue;

                        var tbl = tables[operand];
                        var row = tbl.AsEnumerable()
                            .FirstOrDefault(r => NormalizeDate(r["Chukì"]?.ToString()) == day && !IsHeaderRow(r));

                        double.TryParse(NormalizeNumber(row?[col]?.ToString()), NumberStyles.Any, CultureInfo.InvariantCulture, out double val);

                        if (i == 0)
                            result = val;
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
                    if (result.HasValue) sum += result.Value;
                }

                newRow["Tổng"] = sum.ToString("0.###", CultureInfo.InvariantCulture);
                resultTable.Rows.Add(newRow);
            }

            // ===== TẠO BẢNG TRÊN SQL =====
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
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);

            await bulkCopy.WriteToServerAsync(resultTable);
        }


        private string NormalizeNumber(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return "0";
            raw = raw.Replace(" ", "").Trim();

            if (Regex.IsMatch(raw, @"^\d{1,3}(\.\d{3})+,\d+$"))
            {
                raw = raw.Replace(".", "").Replace(",", ".");
            }
            else if (Regex.IsMatch(raw, @"^\d{1,3}(,\d{3})+\.\d+$"))
            {
                raw = raw.Replace(",", "");
            }
            else
            {
                raw = raw.Replace(",", ".");
            }

            return raw;
        }
        private string NormalizeDate(object? input)
        {
            if (input == null) return "";

            string raw = input.ToString()?.Trim() ?? "";

            string[] formats = { "dd/MM/yyyy", "d/M/yyyy", "M/d/yyyy", "yyyy-MM-dd" };

            if (DateTime.TryParseExact(raw, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                return dt.ToString("d/M/yyyy", CultureInfo.InvariantCulture);

            if (DateTime.TryParse(raw, out dt))
                return dt.ToString("d/M/yyyy", CultureInfo.InvariantCulture);

            return raw;
        }
        private bool IsHeaderRow(DataRow row)
        {
            var chuki = row.Table.Columns.Contains("Chukì") ? row["Chukì"]?.ToString()?.Trim() : null;
            var col2 = row.Table.Columns.Contains("Col2") ? row["Col2"]?.ToString()?.Trim() : null;

            return chuki == "Ngày" || col2 == "Thứ" ||
                   int.TryParse(chuki, out _) || int.TryParse(col2, out _);
        }

    }
}