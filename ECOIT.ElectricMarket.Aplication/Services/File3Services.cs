using ECOIT.ElectricMarket.Application.DTO;
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

    }
}
