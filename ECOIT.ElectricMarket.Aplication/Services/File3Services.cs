using ECOIT.ElectricMarket.Application.Interface;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Text.RegularExpressions;

namespace ECOIT.ElectricMarket.Application.Services
{
    public class File3Services : IFile3Services
    {
        private readonly string _connectionString;

        public File3Services(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection");
        }
        public async Task Tinh3Gia(string tableName)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dt = new DataTable();
            using (var adapter = new SqlDataAdapter($"SELECT * FROM [{tableName}]", conn))
            {
                adapter.FillSchema(dt, SchemaType.Source);
                adapter.Fill(dt);
            }

            if (dt.Rows.Count > 0 && dt.Rows[0]["Chukì"].ToString()?.Trim().ToLower() == "ngày")
            {
                dt.Rows[0].Delete();
                dt.AcceptChanges();
            }

            var colsTD = GetCotTongTD(dt);
            var colsCD = GetCotCaoDiem(dt);

            var dtResult = new DataTable();
            dtResult.Columns.Add("Chukì", typeof(string));
            dtResult.Columns.Add("Thứ", typeof(string));
            dtResult.Columns.Add("BT", typeof(string));
            dtResult.Columns.Add("CD", typeof(string));
            dtResult.Columns.Add("TD", typeof(string));

            foreach (DataRow row in dt.Rows)
            {
                string col2 = row["Col2"]?.ToString()?.Trim();
                string chuki = row["Chukì"]?.ToString();

                decimal td = TinhTong(row, colsTD);
                decimal cd = col2?.ToLower() == "sunday" ? 0 : TinhTong(row, colsCD);

                decimal tong = 0;
                if (decimal.TryParse(row["Tổng"]?.ToString(), out var tongFromData))
                    tong = tongFromData;

                decimal bt = tong - td - cd;

                var newRow = dtResult.NewRow();
                newRow["Chukì"] = chuki;
                newRow["Thứ"] = col2;

                var culture = new CultureInfo("en-US");

                newRow["BT"] = bt.ToString("#,##0", culture);
                newRow["CD"] = cd.ToString("#,##0", culture);
                newRow["TD"] = td.ToString("#,##0", culture);

                dtResult.Rows.Add(newRow);
            }

            string newTableName = "3Gia_" + tableName;

            var createSql = @$"
                IF OBJECT_ID('{newTableName}', 'U') IS NOT NULL DROP TABLE [{newTableName}];
                CREATE TABLE [{newTableName}] (
                    [Chukì] NVARCHAR(100),
                    [Thứ] NVARCHAR(20),
                    [BT] NVARCHAR(20),
                    [CD] NVARCHAR(20),
                    [TD] NVARCHAR(20)
                )";

            using (var cmd = new SqlCommand(createSql, conn))
                await cmd.ExecuteNonQueryAsync();

            using (var bulk = new SqlBulkCopy(conn))
            {
                bulk.DestinationTableName = $"[{newTableName}]";
                foreach (DataColumn col in dtResult.Columns)
                    bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                await bulk.WriteToServerAsync(dtResult);
            }
        }

        private decimal TinhTong(DataRow row, List<string> columnNames)
        {
            decimal sum = 0;
            foreach (var col in columnNames)
            {
                if (decimal.TryParse(row[col]?.ToString(), out var val))
                    sum += val;
            }
            return Math.Round(sum, 5);
        }

        private List<string> GetCotTongTD(DataTable table)
        {
            var columns = new List<string>();
            bool inFirst = false, inSecond = false;

            foreach (DataColumn col in table.Columns)
            {
                var name = col.ColumnName.Trim();

                if (name == "0h30") inFirst = true;
                if (name == "4h30") inFirst = false;

                if (name == "22h30") inSecond = true;
                if (name == "24h")
                {
                    columns.Add(name);
                    break;
                }

                if (inFirst || inSecond)
                    columns.Add(name);
            }

            return columns;
        }

        private List<string> GetCotCaoDiem(DataTable table)
        {
            var columns = new List<string>();
            bool inCD1 = false, inCD2 = false;

            foreach (DataColumn col in table.Columns)
            {
                var name = col.ColumnName.Trim();

                if (name == "10h") inCD1 = true;
                if (name == "12h") inCD1 = false;

                if (name == "17h30") inCD2 = true;
                if (name == "20h")
                {
                    columns.Add(name);
                    break;
                }

                if (inCD1 || inCD2)
                    columns.Add(name);
            }

            return columns;
        }

        public async Task CalculateMultiTableFormulaAsync(string outputTable, string outputLabel, string formula, List<string> sourceTables)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var safeTableName = Regex.Replace(outputTable, "\\W+", "");

            var checkCmd = new SqlCommand("IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
            var exists = (int)await checkCmd.ExecuteScalarAsync();
            if (exists == 1) throw new Exception($"Bảng '{safeTableName}' đã tồn tại.");

            var parts = formula.Split('=');
            if (parts.Length != 2) throw new Exception("Công thức không hợp lệ.");
            var right = parts[1].Trim();

            var operands = Regex.Split(right, @"[+\-\*/]").Select(x => x.Trim()).ToList();
            var operators = Regex.Matches(right, @"[+\-\*/]").Cast<Match>().Select(m => m.Value).ToList();

            var tables = new Dictionary<string, DataTable>();
            foreach (var tbl in sourceTables.Distinct())
            {
                var dt = new DataTable();
                using var cmd = new SqlCommand($"SELECT * FROM [{tbl}]", conn);
                using var adapter = new SqlDataAdapter(cmd);
                adapter.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    var firstRow = dt.Rows[0];
                    if (dt.Columns.Cast<DataColumn>().Any(c => firstRow[c]?.ToString()?.Trim().ToLower() == "ngày"))
                    {
                        dt.Rows[0].Delete();
                        dt.AcceptChanges();
                    }
                }

                tables[tbl] = dt;
            }

            var baseTable = tables[operands[0]];
            var resultTable = baseTable.Clone();
            resultTable.Clear();

            for (int rowIndex = 0; rowIndex < baseTable.Rows.Count; rowIndex++)
            {
                var row = baseTable.Rows[rowIndex];
                var newRow = resultTable.NewRow();

                foreach (DataColumn col in baseTable.Columns)
                {
                    var colName = col.ColumnName;

                    if (colName == "Chukì")
                    {
                        newRow[colName] = NormalizeDate(row[colName]);
                        continue;
                    }
                    if (colName == "Col2")
                    {
                        newRow[colName] = row[colName];
                        continue;
                    }

                    if (colName == "Tổng") continue;

                    decimal result = 0;

                    for (int i = 0; i < operands.Count; i++)
                    {
                        var operandTable = tables[operands[i]];
                        decimal value = 0;

                        if (operandTable.Columns.Contains(colName) && operandTable.Rows.Count > rowIndex)
                        {
                            var obj = operandTable.Rows[rowIndex][colName];
                            decimal.TryParse(obj?.ToString(), out value);
                        }

                        if (i == 0)
                            result = value;
                        else
                        {
                            var op = operators[i - 1];
                            result = op switch
                            {
                                "+" => result + value,
                                "-" => result - value,
                                "*" => result * value,
                                "/" => value != 0 ? result / value : 0,
                                _ => result
                            };
                        }
                    }

                    newRow[colName] = result.ToString("#,##0", CultureInfo.InvariantCulture);
                }

                decimal totalRow = 0;
                foreach (DataColumn col in baseTable.Columns)
                {
                    var colName = col.ColumnName;
                    if (colName == "Chukì" || colName == "Col2" || colName == "Tổng") continue;

                    var valStr = newRow[colName]?.ToString();
                    if (decimal.TryParse(valStr?.Replace(",", ""), out var val))
                    {
                        totalRow += val;
                    }
                }
                newRow["Tổng"] = totalRow.ToString("#,##0", CultureInfo.InvariantCulture);

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
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            await bulkCopy.WriteToServerAsync(resultTable);
        }

        public async Task ExtractSundayRowsAsync(string sourceTable, string targetTable)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var safeSource = Regex.Replace(sourceTable, @"[^\w]", "");
            var safeTarget = Regex.Replace(targetTable, @"[^\w]", "");

            var checkTableCmd = new SqlCommand(
                $"IF OBJECT_ID('{safeTarget}', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);

            var exists = (int)await checkTableCmd.ExecuteScalarAsync();

            if (exists == 0)
            {
                var createCmd = new SqlCommand($@"
                SELECT * INTO [{safeTarget}]
                FROM [{safeSource}]
                WHERE Col2 = 'Sunday'
                ", conn);
                await createCmd.ExecuteNonQueryAsync();
            }
            else
            {
                var insertCmd = new SqlCommand($@"
                INSERT INTO [{safeTarget}]
                SELECT * FROM [{safeSource}]
                WHERE Col2 = 'Sunday'
                AND NOT EXISTS (
                SELECT 1 FROM [{safeTarget}]
                WHERE [{safeTarget}].Chukì = [{safeSource}].Chukì
                )
                ", conn);
                await insertCmd.ExecuteNonQueryAsync();
            }
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

        public async Task CalculateSanluongNTT(string table1, string table2, string outputTable)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dt1 = new DataTable();
            using (var cmd1 = new SqlCommand($"SELECT * FROM [{table1}]", conn))
            using (var ad1 = new SqlDataAdapter(cmd1))
            {
                ad1.Fill(dt1);
            }

            var dt2 = new DataTable();
            using (var cmd2 = new SqlCommand($"SELECT * FROM [{table2}]", conn))
            using (var ad2 = new SqlDataAdapter(cmd2))
            {
                ad2.Fill(dt2);
            }

            var result = new DataTable();
            result.Columns.Add("Ngày");
            result.Columns.Add("Thứ");

            var timeCols = dt1.Columns.Cast<DataColumn>()
                .Where(c => c.ColumnName != "Ngày" && c.ColumnName != "Thứ")
                .Select(c => c.ColumnName)
                .ToList();

            foreach (var col in timeCols)
                result.Columns.Add(col, typeof(string));

            foreach (DataRow row1 in dt1.Rows)
            {
                var date = row1["Ngày"].ToString();
                var row2 = dt2.AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == date);
                if (row2 == null) continue;

                var newRow = result.NewRow();
                newRow["Ngày"] = date;
                newRow["Thứ"] = row1["Thứ"]?.ToString();

                foreach (var col in timeCols)
                {
                    double.TryParse(row1[col]?.ToString(), out double val1);
                    double.TryParse(row2[col]?.ToString(), out double val2);
                    var product = val1 * val2;
                    newRow[col] = product.ToString("0.###", CultureInfo.InvariantCulture);
                }

                result.Rows.Add(newRow);
            }

            var columnsSql = result.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
            var createSql = $"CREATE TABLE [{outputTable}] ({string.Join(", ", columnsSql)})";
            using (var createCmd = new SqlCommand(createSql, conn))
            {
                await createCmd.ExecuteNonQueryAsync();
            }

            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = outputTable };
            foreach (DataColumn col in result.Columns)
            {
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }
            await bulk.WriteToServerAsync(result);
        }

        public async Task CalculateSanluongBTS(string table1, string table2, string outputTable)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dt1 = new DataTable();
            using (var cmd1 = new SqlCommand($"SELECT * FROM [{table1}]", conn))
            using (var ad1 = new SqlDataAdapter(cmd1))
            {
                ad1.Fill(dt1);
            }

            var dt2 = new DataTable();
            using (var cmd2 = new SqlCommand($"SELECT * FROM [{table2}]", conn))
            using (var ad2 = new SqlDataAdapter(cmd2))
            {
                ad2.Fill(dt2);
            }

            if (dt1.Columns.Contains("Chukì"))
                dt1.Columns["Chukì"].ColumnName = "Ngày";
            if (dt1.Columns.Contains("Col2"))
                dt1.Columns["Col2"].ColumnName = "Thứ";
            if (dt2.Columns.Contains("Col2"))
                dt2.Columns["Col2"].ColumnName = "Thứ";

            foreach (DataRow row in dt1.Rows)
            {
                if (DateTime.TryParseExact(row["Ngày"]?.ToString(), "d/M/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d))
                    row["Ngày"] = d.ToString("yyyy-MM-dd");
            }

            foreach (DataRow row in dt2.Rows)
            {
                if (DateTime.TryParseExact(row["Ngày"]?.ToString(), "M/d/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d))
                    row["Ngày"] = d.ToString("yyyy-MM-dd");
            }

            var result = new DataTable();
            result.Columns.Add("Ngày");
            result.Columns.Add("Thứ");

            var timeCols = dt1.Columns.Cast<DataColumn>()
                .Where(c => c.ColumnName != "Ngày" && c.ColumnName != "Thứ")
                .Select(c => c.ColumnName)
                .ToList();

            foreach (var col in timeCols)
                result.Columns.Add(col, typeof(string));

            foreach (DataRow row1 in dt1.Rows)
            {
                var date = row1["Ngày"]?.ToString();
                var row2 = dt2.AsEnumerable().FirstOrDefault(r => r["Ngày"]?.ToString() == date);

                var newRow = result.NewRow();
                newRow["Ngày"] = NormalizeDate(date);
                newRow["Thứ"] = row1["Thứ"]?.ToString();

                foreach (var col in timeCols)
                {
                    double.TryParse(row1[col]?.ToString(), out double val1);
                    double.TryParse(row2[col]?.ToString(), out double val2);
                    var product = val1 - val2;
                    newRow[col] = product.ToString("#,##0", CultureInfo.InvariantCulture);
                }

                result.Rows.Add(newRow);
            }

            var columnsSql = result.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
            var createSql = $"CREATE TABLE [{outputTable}] ({string.Join(", ", columnsSql)})";
            using (var createCmd = new SqlCommand(createSql, conn))
            {
                await createCmd.ExecuteNonQueryAsync();
            }

            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = outputTable };
            foreach (DataColumn col in result.Columns)
            {
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }
            await bulk.WriteToServerAsync(result);
        }

        public async Task CreateHeSoKAsync()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dt = new DataTable();
            using (var cmd = new SqlCommand("SELECT * FROM [NhapgiaNM] WHERE Giá = @gia", conn))
            {
                cmd.Parameters.AddWithValue("@gia", "k");
                using (var adapter = new SqlDataAdapter(cmd))
                {
                    adapter.Fill(dt);
                }
            }

            using (var cmdCreate = new SqlCommand
            (@"
                IF OBJECT_ID('dbo.HeSoK', 'U') IS NOT NULL DROP TABLE dbo.HeSoK;

                SELECT * INTO HeSoK FROM [NhapgiaNM] WHERE 1 = 0;
                ", conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = "HeSoK";
                await bulkCopy.WriteToServerAsync(dt);
            }
        }

        public async Task TinhTyLe()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var alterCmd = new SqlCommand(@"
                IF COL_LENGTH('TyLeSanLuong', 'TongSanLuong') IS NULL
                BEGIN
                    ALTER TABLE TyLeSanLuong ADD TongSanLuong DECIMAL(18, 3)
                END
            ", conn);
            await alterCmd.ExecuteNonQueryAsync();

            var dt = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT * FROM TyLeSanLuong", conn))
            {
                adapter.Fill(dt);
            }

            foreach (DataRow row in dt.Rows)
            {
                decimal s1 = TryParseDecimal(row["DHAI3MRTBINHVTAN4_SảnlượngMWh"]);
                decimal s2 = TryParseDecimal(row["S1_SảnlượngMWh"]);
                decimal s3 = TryParseDecimal(row["S2_SảnlượngMWh"]);
                decimal tong = s1 + s2 + s3;

                var cmd = new SqlCommand(@"
                UPDATE TyLeSanLuong
                SET TongSanLuong = @Tong
                WHERE Ngày = @Ngay AND ChuKì = @ChuKi", conn);

                cmd.Parameters.AddWithValue("@Tong", tong);
                cmd.Parameters.AddWithValue("@Ngay", row["Ngày"]);
                cmd.Parameters.AddWithValue("@ChuKi", row["ChuKì"]);

                await cmd.ExecuteNonQueryAsync();
            }
        }

        private decimal TryParseDecimal(object? value)
        {
            if (value == null || value == DBNull.Value)
                return 0m;

            decimal.TryParse(value.ToString(), out var result);
            return result;
        }

    }
}