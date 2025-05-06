using ECOIT.ElectricMarket.Application.Interface;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Text.RegularExpressions;

namespace ECOIT.ElectricMarket.Application.Services
{
    public class CaculateServices : ICaculateServices
    {
        private readonly string _connectionString;

        public CaculateServices(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection");
        }

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

        private static string FormatToThousandVN(double value)
        {
            var rounded = Math.Round(value);

            var divided = rounded / 1000.0;

            var formatted = divided.ToString("0.###", CultureInfo.InvariantCulture);

            return formatted.Replace(".", ",");
        }

        public async Task CaculatePmAsync(string tableFMP, string tableA0, string resultTableName = "SaiKhac(PM)")
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var safeTableName = Regex.Replace(resultTableName, @"\W+", "");
            var checkResultTableCmd = new SqlCommand(@"
            IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkResultTableCmd.Parameters.AddWithValue("@tableName", safeTableName);
            var resultExists = (int)await checkResultTableCmd.ExecuteScalarAsync();

            if (resultExists == 1)
                throw new Exception($"Pm đã được tính trước đó, vui lòng kiểm tra lại.");

            var checkFmpCmd = new SqlCommand(@"
            IF OBJECT_ID(@tableFMP, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkFmpCmd.Parameters.AddWithValue("@tableFMP", tableFMP);
            var fmpExists = (int)await checkFmpCmd.ExecuteScalarAsync();

            if (fmpExists == 0)
                throw new Exception($" Bảng '{tableFMP}' không tồn tại. Vui lòng kiểm tra lại."); ;

            var checkA0Cmd = new SqlCommand($@"
            IF OBJECT_ID(N'{tableA0}', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            var a0Exists = (int)await checkA0Cmd.ExecuteScalarAsync();

            if (a0Exists == 0)
                throw new Exception($" Bảng '{tableA0}' không tồn tại. Vui lòng kiểm tra lại.");

            var dfFMP = new DataTable();
            var dfA0 = new DataTable();

            using (var cmd = new SqlCommand($"SELECT * FROM [{tableFMP}]", conn))
            using (var adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(dfFMP);
            }

            using (var cmd = new SqlCommand($"SELECT * FROM [{tableA0}]", conn))
            using (var adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(dfA0);
            }

            var diffTable = dfFMP.Clone();

            for (int i = 0; i < dfFMP.Rows.Count; i++)
            {
                var newRow = diffTable.NewRow();
                newRow["Ngày"] = dfFMP.Rows[i]["Ngày"];
                newRow["Giá"] = "Pm";

                var sharedColumns = dfFMP.Columns
                    .Cast<DataColumn>()
                    .Where(c => c.ColumnName != "Ngày" && c.ColumnName != "Giá" && dfA0.Columns.Contains(c.ColumnName))
                    .ToList();

                var ngay = dfFMP.Rows[i]["Ngày"]?.ToString();

                foreach (var col in sharedColumns)
                {
                    var fmpRaw = NormalizeNumber(dfFMP.Rows[i][col.ColumnName]?.ToString() ?? "0");
                    var a0Raw = NormalizeNumber(dfA0.Rows[i][col.ColumnName]?.ToString() ?? "0");

                    double.TryParse(fmpRaw, NumberStyles.Any, CultureInfo.InvariantCulture, out double fmp);
                    double.TryParse(a0Raw, NumberStyles.Any, CultureInfo.InvariantCulture, out double a0);

                    var diff = fmp - a0;
                    //Console.WriteLine($" Ngày {ngay} |  Giờ {col.ColumnName} | FMP = {fmp} - A0 = {a0} → Sai khác = {diff}");

                    newRow[col.ColumnName] = diff.ToString("0.###", CultureInfo.InvariantCulture);
                }

                diffTable.Rows.Add(newRow);
            }

            var columnsSql = diffTable.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
            var createSql = $"CREATE TABLE [{safeTableName}] ({string.Join(", ", columnsSql)})";

            using (var createCmd = new SqlCommand(createSql, conn))
            {
                await createCmd.ExecuteNonQueryAsync();
            }

            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = safeTableName
            };

            foreach (DataColumn col in diffTable.Columns)
            {
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            }

            await bulkCopy.WriteToServerAsync(diffTable);
        }

        public async Task CalculateFmpAsync()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(@"
            IF OBJECT_ID('FMP', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            var exists = (int)await checkCmd.ExecuteScalarAsync();

            if (exists == 1)
            {
                throw new Exception(" FMP đã tính, vui lòng kiểm tra lại");
            }

            var checkTableCmd = new SqlCommand(@"
            IF OBJECT_ID('NhapgiaNM', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            var tableExists = (int)await checkTableCmd.ExecuteScalarAsync();

            if (tableExists == 0)
            {
                throw new Exception(" Bảng 'NhapgiaNM' không tồn tại. Vui lòng import dữ liệu trước.");
            }

            var rawData = new DataTable();
            var cmd = new SqlCommand(@"
            SELECT * FROM NhapgiaNM
            WHERE LTRIM(RTRIM(Giá)) IN ('SMP', 'CAN')", conn);
            using (var adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(rawData);
            }

            var timeCols = rawData.Columns.Cast<DataColumn>()
                .Select(c => c.ColumnName)
                .Where(c => c != "Ngày" && c != "Giá")
                .ToList();

            var smpRows = rawData.AsEnumerable()
                .Where(r => r["Giá"].ToString().Trim() == "SMP"
                         && !string.IsNullOrWhiteSpace(r["Ngày"]?.ToString()))
                .ToList();

            var canRows = rawData.AsEnumerable()
                .Where(r => r["Giá"].ToString().Trim() == "CAN"
                         && !string.IsNullOrWhiteSpace(r["Ngày"]?.ToString()))
                .ToList();

            var joined = from smp in smpRows
                         join can in canRows
                         on smp["Ngày"].ToString().Trim() equals can["Ngày"].ToString().Trim()
                         select new { Ngay = smp["Ngày"].ToString(), RowSMP = smp, RowCAN = can };

            var fmpTable = new DataTable();
            fmpTable.Columns.Add("Ngày", typeof(string));
            fmpTable.Columns.Add("Giá", typeof(string));
            foreach (var col in timeCols)
                fmpTable.Columns.Add(col, typeof(string));

            foreach (var pair in joined)
            {
                var newRow = fmpTable.NewRow();
                newRow["Ngày"] = pair.Ngay;
                newRow["Giá"] = "FMP";

                foreach (var col in timeCols)
                {
                    var smpStr = NormalizeNumber(pair.RowSMP[col]?.ToString());
                    var canStr = NormalizeNumber(pair.RowCAN[col]?.ToString());

                    double.TryParse(smpStr, NumberStyles.Any, CultureInfo.InvariantCulture, out double smpVal);
                    double.TryParse(canStr, NumberStyles.Any, CultureInfo.InvariantCulture, out double canVal);

                    double fmpVal = Math.Round(smpVal + canVal, 4);
                    newRow[col] = fmpVal.ToString("0.###", CultureInfo.InvariantCulture);
                }

                fmpTable.Rows.Add(newRow);
            }

            var createCmd = new SqlCommand($@"
            CREATE TABLE FMP (
                Ngày NVARCHAR(50),
                Giá NVARCHAR(50),
                {string.Join(", ", timeCols.Select(c => $"[{c}] NVARCHAR(50)"))}
            )", conn);
            await createCmd.ExecuteNonQueryAsync();

            using var bulk = new SqlBulkCopy(conn);
            bulk.DestinationTableName = "FMP";
            foreach (DataColumn col in fmpTable.Columns)
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

            await bulk.WriteToServerAsync(fmpTable);
        }

        public async Task CalculateCFMPAsync()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCFMPCmd = new SqlCommand(@"IF OBJECT_ID('PM_CFMP', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            var cfmpExists = (int)await checkCFMPCmd.ExecuteScalarAsync();
            if (cfmpExists == 1)
            {
                throw new Exception(" CFMP đã tính, vui lòng kiểm tra lại");
            }

            var checkNM = new SqlCommand(@"IF OBJECT_ID('NhapgiaNM', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            var nmExists = (int)await checkNM.ExecuteScalarAsync();
            if (nmExists == 0)
            {
                throw new Exception(" Bảng 'NhapgiaNM' không tồn tại. Vui lòng import dữ liệu trước.");
            }

            var checkFMP = new SqlCommand(@"IF OBJECT_ID('FMP', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            var fmpExists = (int)await checkFMP.ExecuteScalarAsync();
            if (fmpExists == 0)
            {
                throw new Exception(" Bảng 'FMP' không tồn tại. Vui lòng tính FMP trước.");
            }

            var dfFMP = new DataTable();
            var dfNM = new DataTable();

            using (var cmd = new SqlCommand("SELECT * FROM FMP", conn))
            using (var adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(dfFMP);
            }

            using (var cmd = new SqlCommand("SELECT * FROM NhapgiaNM", conn))
            using (var adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(dfNM);
            }

            var dfCFMP = dfFMP.Clone();

            for (int i = 0; i < dfFMP.Rows.Count; i++)
            {
                var newRow = dfCFMP.NewRow();
                newRow["Ngày"] = dfFMP.Rows[i]["Ngày"];
                newRow["Giá"] = "PM";

                var ngay = dfFMP.Rows[i]["Ngày"]?.ToString();

                var rowK = dfNM.AsEnumerable()
                    .FirstOrDefault(r => r["Ngày"]?.ToString() == ngay &&
                                         r["Giá"]?.ToString()?.Trim() == "k");

                var sharedColumns = dfFMP.Columns
                    .Cast<DataColumn>()
                    .Where(c => c.ColumnName != "Ngày" && c.ColumnName != "Giá" && dfNM.Columns.Contains(c.ColumnName))
                    .ToList();

                foreach (var col in sharedColumns)
                {
                    var fmpRaw = NormalizeNumber(dfFMP.Rows[i][col.ColumnName]?.ToString() ?? "0");
                    var kRaw = NormalizeNumber(rowK?[col.ColumnName]?.ToString() ?? "0");

                    double.TryParse(fmpRaw, NumberStyles.Any, CultureInfo.InvariantCulture, out double fmp);
                    double.TryParse(kRaw, NumberStyles.Any, CultureInfo.InvariantCulture, out double k);

                    var cfmp = fmp * k;
                    Console.WriteLine($"👉 Ngày {dfFMP.Rows[i]["Ngày"]} | Giờ {col.ColumnName} | FMP = {fmp} | k = {k} → CFMP = {cfmp}");
                    newRow[col.ColumnName] = FormatToThousandVN(cfmp);
                }
                dfCFMP.Rows.Add(newRow);
            }

            var tableName = "PM_CFMP";

            var columnsSql = dfCFMP.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");

            var createSql = $"CREATE TABLE [{tableName}] ({string.Join(", ", columnsSql)})";

            using (var createCmd = new SqlCommand(createSql, conn))
            {
                await createCmd.ExecuteNonQueryAsync();
            }

            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = tableName;

                foreach (DataColumn col in dfCFMP.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dfCFMP);
            }
        }

        public async Task CaculatePmCFMPAsync()
        {
            var tableFMP = "PM_CFMP";
            var tableA0 = "GiaA0congboPM";
            var resultTableName = "SaiKhacPMCFMP";

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            // Nếu bảng kết quả đã tồn tại thì dừng lại
            var checkResultTableCmd = new SqlCommand(@"
        IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkResultTableCmd.Parameters.AddWithValue("@tableName", resultTableName);
            var resultExists = (int)await checkResultTableCmd.ExecuteScalarAsync();
            if (resultExists == 1)
                throw new Exception($" Bảng '{resultTableName}' đã tồn tại. Pm đã được tính trước đó.");

            // Load dữ liệu từ hai bảng
            var dfFMP = new DataTable();
            var dfA0 = new DataTable();

            using (var cmd = new SqlCommand($"SELECT * FROM [{tableFMP}]", conn))
            using (var adapter = new SqlDataAdapter(cmd))
                adapter.Fill(dfFMP);

            using (var cmd = new SqlCommand($"SELECT * FROM [{tableA0}]", conn))
            using (var adapter = new SqlDataAdapter(cmd))
                adapter.Fill(dfA0);

            // Clone cấu trúc bảng và tạo bảng kết quả
            var diffTable = dfFMP.Clone();
            diffTable.TableName = resultTableName;

            for (int i = 0; i < dfFMP.Rows.Count; i++)
            {
                var newRow = diffTable.NewRow();
                newRow["Ngày"] = dfFMP.Rows[i]["Ngày"];
                newRow["Giá"] = "PM";

                foreach (DataColumn col in dfFMP.Columns)
                {
                    if (col.ColumnName == "Ngày" || col.ColumnName == "Giá")
                        continue;

                    var fmpRaw = dfFMP.Rows[i][col.ColumnName]?.ToString() ?? "0";
                    var a0Raw = dfA0.Rows[i][col.ColumnName]?.ToString() ?? "0";

                    fmpRaw = NormalizeNumber(fmpRaw);
                    a0Raw = NormalizeNumber(a0Raw);

                    double.TryParse(fmpRaw, NumberStyles.Any, CultureInfo.InvariantCulture, out var fmp);
                    double.TryParse(a0Raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var a0);

                    var diff = fmp - a0;
                    newRow[col.ColumnName] = diff.ToString("0.###", CultureInfo.InvariantCulture);
                }

                diffTable.Rows.Add(newRow);
            }

            // Tạo bảng kết quả trong SQL Server
            var columnsSql = diffTable.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
            var createSql = $"CREATE TABLE [{resultTableName}] ({string.Join(", ", columnsSql)})";

            using (var createCmd = new SqlCommand(createSql, conn))
                await createCmd.ExecuteNonQueryAsync();

            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = resultTableName
            };

            foreach (DataColumn col in diffTable.Columns)
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);

            await bulkCopy.WriteToServerAsync(diffTable);
        }
    }
}