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

                var day = NormalizeDate(qmRow["Chukì"]?.ToString());
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


        //public async Task CreateTongHopCsportAsync()
        //{
        //    var tableMapping = new Dictionary<string, string[]>
        //    {
        //        //sản lượng
        //        { "QM1", new[] { "QM1_24Chuky" } },
        //        { "QM2_ThaiBinh", new[] { "QM2_ThaiBinh_24Chuky" } },
        //        { "QM2_VinhTan4", new[] { "QM2_VinhTan4_24Chuky" } },
        //        { "QM2_VinhTan4_MR", new[] { "QM2_VinhTan4_MR_24Chuky" } },
        //        { "QM2_DuyenHai3_MR", new[] { "QM2_DuyenHai3_MR_24Chuky" } },
        //        //chi phí
        //        { "Cspot1", new[] { "Cspot_PhúMỹ" } },
        //        { "Cspot2_ThaiBinh", new[] { "Cspot_ThaiBinh" } },
        //        { "Cspot2_VinhTan4", new[] { "Cspot_VinhTan4 "} },
        //        { "Cspot2_VinhTan4_MR", new[] { "Cspot_VinhTan4_MR" } },
        //        { "Cspot2_DuyenHai3_MR", new[] { "Csport_DuyenHai3_MR" } },
        //    };

        //    using var conn = new SqlConnection(_connectionString);
        //    await conn.OpenAsync();

        //    var allData = new Dictionary<string, DataTable>();

        //    foreach (var kv in tableMapping)
        //    {
        //        var dt = new DataTable();
        //        using var cmd = new SqlCommand($"SELECT Ngày, Tổng FROM [{kv.Value[0]}]", conn);
        //        using var adapter = new SqlDataAdapter(cmd);
        //        allData[kv.Key] = dt;
        //    }

        //    var allDays = allData.Values
        //        .SelectMany(dt => dt.AsEnumerable().Select(row => row.Field<string>("Ngày")))
        //        .Distinct()
        //        .OrderBy(d => DateTime.ParseExact(d, "dd/MM/yyyy", CultureInfo.InvariantCulture))
        //        .ToList();

        //    var result = new DataTable();

        //    result.Columns.Add("Ngày", typeof(string));
        //    result.Columns.Add("QM1", typeof(string));
        //    result.Columns.Add("QM2", typeof(string));
        //    result.Columns.Add("QM", typeof(string));

        //    result.Columns.Add("Cspot1", typeof(string));
        //    result.Columns.Add("Cspot2", typeof(string));
        //    result.Columns.Add("TongCspot", typeof(string));

        //    var qm2Parts = new[]
        //    {
        //        "QM2_ThaiBinh",
        //        "QM2_VinhTan4",
        //        "QM2_VinhTan4_MR",
        //        "QM2_DuyenHai3_MR"
        //    };

        //    var csport2Parts = new[]
        //    {
        //        "Cspot2_ThaiBinh",
        //        "Cspot2_VinhTan4",
        //        "Cspot2_VinhTan4_MR",
        //        "Cspot2_DuyenHai3_MR"
        //    };

        //    foreach (var col in qm2Parts.Concat(csport2Parts)) 
        //    {
        //        result.Columns.Add(col, typeof(string));
        //    }
             
        //}


        public async Task CreateTongHopCsportAsync()
        {
            var tongHopTables = new Dictionary<string, string[]>
            {
                // Sản lượng
                { "QM1", new[] { "QM1_24Chuky" } },
                { "QM2_ThaiBinh", new[] { "QM2_ThaiBinh_24Chuky" } },
                { "QM2_VinhTan4", new[] { "QM2_VinhTan4_24Chuky" } },
                { "QM2_VinhTan4_MR", new[] { "QM2_VinhTan4_MR_24Chuky" } },
                { "QM2_DuyenHai3_MR", new[] { "QM2_DuyenHai3_MR_24Chuky" } },

                // Chi phí
                { "Cspot1", new[] { "Cspot_PhúMỹ" } },
                { "Cspot2_ThaiBinh", new[] { "Cspot_ThaiBinh" } },
                { "Cspot2_VinhTan4", new[] { "Cspot_VinhTan4" } },
                { "Cspot2_VinhTan4_MR", new[] { "Cspot_VinhTan4_MR" } },
                { "Cspot2_DuyenHai3_MR", new[] { "Csport_DuyenHai3_MR" } }
            };

            var qm2Parts = new[] { "QM2_ThaiBinh", "QM2_VinhTan4", "QM2_VinhTan4_MR", "QM2_DuyenHai3_MR" };
            var cspot2Parts = new[] { "Cspot2_ThaiBinh", "Cspot2_VinhTan4", "Cspot2_VinhTan4_MR", "Cspot2_DuyenHai3_MR" };

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            // Load dữ liệu
            var data = new Dictionary<string, DataTable>();
            foreach (var kv in tongHopTables)
            {
                var dt = new DataTable();
                using var cmd = new SqlCommand($"SELECT Ngày, Tổng FROM [{kv.Value[0]}]", conn);
                using var adapter = new SqlDataAdapter(cmd);
                adapter.Fill(dt);
                data[kv.Key] = dt;
            }

            // Lấy danh sách tất cả ngày
            var allDays = data.Values
                .SelectMany(dt => dt.AsEnumerable().Select(r => r["Ngày"].ToString()))
                .Distinct()
                .OrderBy(d => d)
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

            // Gộp dữ liệu theo ngày
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

            // Tính dòng tổng
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

            // Ghi vào bảng SQL
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

            // =======================
            // Helper methods
            // =======================
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


    }
}
