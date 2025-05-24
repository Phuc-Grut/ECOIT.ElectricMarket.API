using ECOIT.ElectricMarket.Application.Interface;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.Services
{
    public class CalculateCsportServices : ICsport
    {
        private readonly string _connectionString;

        public CalculateCsportServices(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection");
        }

        public async Task CalculateCsport1(string tbaleName, string province)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dtPm = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT * FROM PM_CFMP", conn))
            {
                adapter.Fill(dtPm);
            }

            var dtCalcu = new DataTable();
            using (var adapter = new SqlDataAdapter($"SELECT * FROM [{tbaleName}]", conn))
            {
                adapter.Fill(dtCalcu);
            }

            if (dtCalcu.Rows.Count > 0 && IsHeaderRow(dtCalcu.Rows[0]))
            {
                dtCalcu.Rows.RemoveAt(0);
            }

            var timeCols = dtCalcu.Columns.Cast<DataColumn>()
                .Select(c => c.ColumnName)
                .Where(c => !new[] { "Chukì", "Col2", "Ngày", "Giá", "Tổng" }.Contains(c))
                .ToList();

            var dtResult = new DataTable($"Cspot_{province}");
            dtResult.Columns.Add("Ngày");
            foreach (var col in timeCols)
                dtResult.Columns.Add(col);
            dtResult.Columns.Add("Tổng");

            for (int i = 0; i < Math.Min(dtPm.Rows.Count, dtCalcu.Rows.Count); i++)
            {
                var pmRow = dtPm.Rows[i];
                var qmRow = dtCalcu.Rows[i];

                var day = qmRow["Ngày"]?.ToString();

                var newRow = dtResult.NewRow();
                newRow["Ngày"] = day;

                double sum = 0;

                foreach (var col in timeCols)
                {
                    double.TryParse(NormalizeNumber(pmRow[col]?.ToString()), out var pmVal);
                    double.TryParse(NormalizeNumber(qmRow[col]?.ToString()), out var qmVal);

                    var val = pmVal * qmVal;
                    sum += val;
                    Console.WriteLine($" {day} - [{col}]: PM = {pmVal}, QM = {qmVal} → KQ = {val}");
                    newRow[col] = Math.Round(val).ToString("N0", new CultureInfo("en-US"));

                }

                newRow["Tổng"] = Math.Round(sum).ToString("N0", new CultureInfo("en-US"));

                dtResult.Rows.Add(newRow);
            }

            var columnsSql = dtResult.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");

            var createSql = $"CREATE TABLE [Cspot_{province}] ({string.Join(", ", columnsSql)})";
            using (var createCmd = new SqlCommand(createSql, conn))
                await createCmd.ExecuteNonQueryAsync();

            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = $"Cspot_{province}" };
            foreach (DataColumn col in dtResult.Columns)
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

            await bulk.WriteToServerAsync(dtResult);
        }

        private bool IsHeaderRow(DataRow row)
        {
            var val = row[0]?.ToString()?.Trim()?.ToLower();
            return val == "ngày" || val == "chukì" || val == "thứ";
        }

        private string NormalizeNumber(string? input)
        {
            return input?.Replace(",", "").Trim() ?? "0";
        }

        private string NormalizeDate(string? input)
        {
            return DateTime.TryParse(input, out var dt)
                ? dt.ToString("dd/MM/yyyy")
                : input ?? "";
        }

        public async Task CreateTongHopCsportAsync()
        {
            var qm2Parts = new[] { "QM2_ThaiBinh", "QM2_VinhTan4", "QM2_VinhTan4_MR", "QM2_DuyenHai3_MR" };
            var cspot2Parts = new[] { "Cspot2_ThaiBinh", "Cspot2_VinhTan4", "Cspot2_VinhTan4_MR", "Cspot2_DuyenHai3_MR" };

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var data = new Dictionary<string, DataTable>();

            // QM1
            try
            {
                var dt = new DataTable();
                using var cmd = new SqlCommand("SELECT Chukì AS Ngày, Tổng FROM [QM1_24Chuky]", conn);
                using var adapter = new SqlDataAdapter(cmd);
                adapter.Fill(dt);
                data["QM1"] = dt;
            }
            catch (Exception ex)
            {
                throw new Exception("❌ Lỗi khi đọc bảng QM1_24Chuky", ex);
            }

            // QM2
            data["QM2_ThaiBinh"] = await LoadTableAsync(conn, "QM2_ThaiBinh_24Chuky");
            data["QM2_VinhTan4"] = await LoadTableAsync(conn, "QM2_VinhTan4_24Chuky");
            data["QM2_VinhTan4_MR"] = await LoadTableAsync(conn, "QM2_VinhTan4_MR_24Chuky");
            data["QM2_DuyenHai3_MR"] = await LoadTableAsync(conn, "QM2_DuyenHai3_MR_24Chuky");

            // Cspot
            data["Cspot1"] = await LoadTableAsync(conn, "Cspot_PhúMỹ");
            data["Cspot2_ThaiBinh"] = await LoadTableAsync(conn, "Cspot_ThaiBinh");
            data["Cspot2_VinhTan4"] = await LoadTableAsync(conn, "Cspot_VinhTan4");
            data["Cspot2_VinhTan4_MR"] = await LoadTableAsync(conn, "Cspot_VinhTan4_MR");
            data["Cspot2_DuyenHai3_MR"] = await LoadTableAsync(conn, "Cspot_DuyenHai3_MR");

            var allDays = data["Cspot1"].AsEnumerable()
                .Select(r => r["Ngày"].ToString())
                .Where(d => !string.IsNullOrWhiteSpace(d))
                .Distinct()
                .OrderBy(d => DateTime.ParseExact(d, "d/M/yyyy", CultureInfo.InvariantCulture))
                .ToList();

            var result = new DataTable();
            result.Columns.Add("Ngày", typeof(string));
            var columns = new[]
            {
                "QM1", "QM2", "QM",
                "Cspot1", "Cspot2", "Tong_Cspot"
            }.Concat(qm2Parts).Concat(cspot2Parts).ToArray();

            foreach (var col in columns)
                result.Columns.Add(col, typeof(string));


            foreach (var day in allDays)
            {
                var row = result.NewRow();
                row["Ngày"] = day;

                double qm1 = GetValue(data, "QM1", day);
                double qm2 = qm2Parts.Sum(p => GetValue(data, p, day));
                double cspot1 = GetValue(data, "Cspot1", day);
                double cspot2 = cspot2Parts.Sum(p => GetValue(data, p, day));

                row["QM1"] = FormatNum(qm1);
                row["QM2"] = FormatNum(qm2);
                row["QM"] = FormatNum(qm1 + qm2);

                row["Cspot1"] = FormatNum(cspot1);
                row["Cspot2"] = FormatNum(cspot2);
                row["Tong_Cspot"] = FormatNum(cspot1 + cspot2);

                foreach (var p in qm2Parts)
                    row[p] = FormatNum(GetValue(data, p, day));

                foreach (var p in cspot2Parts)
                    row[p] = FormatNum(GetValue(data, p, day));

                result.Rows.Add(row);
            }

            var totalRow = result.NewRow();
            totalRow["Ngày"] = "Tổng";
            foreach (DataColumn col in result.Columns)
            {
                if (col.ColumnName == "Ngày") continue;

                double sum = result.AsEnumerable()
                    .Where(r => r["Ngày"].ToString() != "Tổng")
                    .Sum(r => double.TryParse(r[col.ColumnName]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var v) ? v : 0);

                totalRow[col.ColumnName] = FormatNum(sum);
            }
            result.Rows.Add(totalRow);


            using (var drop = new SqlCommand("IF OBJECT_ID('TongHop_Csport', 'U') IS NOT NULL DROP TABLE TongHop_Csport", conn))
                await drop.ExecuteNonQueryAsync();

            var columnDefs = result.Columns.Cast<DataColumn>().Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
            var createSql = $"CREATE TABLE TongHop_Csport ({string.Join(", ", columnDefs)})";
            using (var createCmd = new SqlCommand(createSql, conn))
                await createCmd.ExecuteNonQueryAsync();

            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = "TongHop_Csport" };
            foreach (DataColumn col in result.Columns)
                bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

            await bulk.WriteToServerAsync(result);


            double GetValue(Dictionary<string, DataTable> dataMap, string key, string day)
            {
                if (!dataMap.TryGetValue(key, out var dt)) return 0;
                var row = dt.AsEnumerable().FirstOrDefault(r => r["Ngày"].ToString() == day);
                if (row != null && double.TryParse(row["Tổng"]?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var val))
                    return val;
                return 0;
            }

            string FormatNum(double num) => num.ToString("#,0", CultureInfo.InvariantCulture);
        }


        private async Task<DataTable> LoadTableAsync(SqlConnection conn, string tableName)
        {
            var dt = new DataTable();
            try
            {
                var isQM = tableName.StartsWith("QM", StringComparison.OrdinalIgnoreCase);
                var timeColumn = isQM ? "Chukì" : "Ngày";

                using var cmd = new SqlCommand($"SELECT {timeColumn} AS Ngày, Tổng FROM [{tableName}]", conn);
                using var adapter = new SqlDataAdapter(cmd);
                adapter.Fill(dt);
                return dt;
            }
            catch (Exception ex)
            {
                throw new Exception($"❌ Lỗi khi đọc bảng {tableName}: {ex.Message}", ex);
            }
        }

    }
}
