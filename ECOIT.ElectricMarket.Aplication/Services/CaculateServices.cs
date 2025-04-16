using ECOIT.ElectricMarket.Application.Interface;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using System.Data;
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

        public async Task CaculatePmAsync(string tableFMP, string tableA0, string resultTableName = "SaiKhac")
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            // 1. Đọc toàn bộ dữ liệu từ 2 bảng
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

            // 2. Tạo bảng sai khác
            var diffTable = dfFMP.Clone();

            // 🔧 Hàm chuẩn hoá số trước khi parse
            string NormalizeNumber(string raw)
            {
                if (string.IsNullOrWhiteSpace(raw)) return "0";
                raw = raw.Trim();

                if (raw.Contains(" ") && raw.Contains(".")) // ví dụ: "1 572.800"
                    raw = raw.Replace(" ", "").Replace(",", "").Replace(".", ",");

                else if (!raw.Contains(",") && raw.Contains(".")) // ví dụ: "1808.9"
                    raw = raw.Replace(".", ",");

                return raw.Replace(",", "."); // để parse bằng InvariantCulture
            }

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
                    Console.WriteLine($"📅 Ngày {ngay} | 🕒 Giờ {col.ColumnName} | FMP = {fmp} - A0 = {a0} → Sai khác = {diff}");

                    newRow[col.ColumnName] = diff.ToString("0.###", CultureInfo.InvariantCulture);
                }

                diffTable.Rows.Add(newRow);
            }

            // 3. Tạo bảng mới và insert vào DB
            var safeTableName = Regex.Replace(resultTableName, @"\W+", "");
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

            var rawData = new DataTable();
            var cmd = new SqlCommand(@"
            SELECT * FROM NhậpgiáNM
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
                    string NormalizeNumber(string raw)
                    {
                        if (string.IsNullOrWhiteSpace(raw)) return "0";
                        raw = raw.Trim();

                        if (raw.Contains(" ") && raw.Contains("."))
                            raw = raw.Replace(" ", "").Replace(",", "").Replace(".", ",");

                        else if (!raw.Contains(",") && raw.Contains("."))
                            raw = raw.Replace(".", ",");

                        return raw.Replace(",", ".");
                    }

                    var smpStr = NormalizeNumber(pair.RowSMP[col]?.ToString());
                    var canStr = NormalizeNumber(pair.RowCAN[col]?.ToString());

                    double.TryParse(smpStr, NumberStyles.Any, CultureInfo.InvariantCulture, out double smpVal);
                    double.TryParse(canStr, NumberStyles.Any, CultureInfo.InvariantCulture, out double canVal);

                    double fmpVal = Math.Round(smpVal + canVal, 4);
                    newRow[col] = fmpVal.ToString("0.###", CultureInfo.InvariantCulture);
                }

                fmpTable.Rows.Add(newRow);
            }

            var checkCmd = new SqlCommand(@"
            IF OBJECT_ID('FMP', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            var exists = (int)await checkCmd.ExecuteScalarAsync();

            if (exists == 0)
            {
                var createCmd = new SqlCommand($@"
            CREATE TABLE FMP (
                Ngày NVARCHAR(50),
                Giá NVARCHAR(50),
                {string.Join(", ", timeCols.Select(c => $"[{c}] NVARCHAR(50)"))}
            )", conn);
                await createCmd.ExecuteNonQueryAsync();
            }

            using var bulk = new SqlBulkCopy(conn);
            bulk.DestinationTableName = "FMP";
            foreach (DataColumn col in fmpTable.Columns)
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

            await bulk.WriteToServerAsync(fmpTable);
        }
    }
}
