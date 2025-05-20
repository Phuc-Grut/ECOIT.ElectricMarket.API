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
    }
}
