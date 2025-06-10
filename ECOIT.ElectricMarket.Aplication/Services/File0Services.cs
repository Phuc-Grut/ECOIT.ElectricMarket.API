using ECOIT.ElectricMarket.Application.Interface;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ECOIT.ElectricMarket.Application.Services
{
    public class File0Services : IFile0Services
    {
        private readonly string _connectionString;

        public File0Services(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection");
        }

        public async Task TinhTong()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dt = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT * FROM Tong_File0", conn))
            {
                adapter.FillSchema(dt, SchemaType.Source);
                adapter.Fill(dt);
            }

            string newColName = "Giá trị tháng 02 tạm tính";

            if (!dt.Columns.Contains(newColName))
                dt.Columns.Add(newColName, typeof(decimal));

            foreach (DataRow row in dt.Rows)
            {
                int stt = 0;
                int.TryParse(row[0]?.ToString(), out stt);
                row[newColName] = 1000000 + stt;
            }

            dt.Columns[newColName].SetOrdinal(3);

            for (int i = 0; i < dt.Columns.Count; i++)
            {
                string colExcelName = GetExcelColumnName(i + 1);
                dt.Columns[i].ColumnName = colExcelName;
            }

            if (dt.Rows.Count >= 3)
            {
                decimal d2 = TryParseDecimal(dt.Rows[1]["D"]);
                decimal d3 = TryParseDecimal(dt.Rows[2]["D"]);
                dt.Rows[0]["D"] = d2 + d3;
            }

            dt.TableName = "Tong_File0";
            PrintDataTable(dt);
        }



        public void PrintDataTable(DataTable dt)
        {
            Console.WriteLine($"\n===== DỮ LIỆU TRONG BẢNG {dt.TableName} =====\n");

            Console.Write("│ STT │");
            foreach (DataColumn col in dt.Columns)
            {
                Console.Write($" {col.ColumnName,-25}│");
            }
            Console.WriteLine();

            Console.WriteLine(new string('─', 6 + dt.Columns.Count * 28));

            int stt = 1;
            foreach (DataRow row in dt.Rows)
            {
                Console.Write($"│ {stt,3} │");
                foreach (var item in row.ItemArray)
                {
                    string value = item?.ToString() ?? "";
                    if (value.Length > 24) value = value.Substring(0, 22) + "..";
                    Console.Write($" {value,-25}│");
                }
                Console.WriteLine();
                stt++;
            }
        }

        public string GetExcelColumnName(int columnNumber)
        {
            string columnName = "";
            while (columnNumber > 0)
            {
                int modulo = (columnNumber - 1) % 26;
                columnName = Convert.ToChar(65 + modulo) + columnName;
                columnNumber = (columnNumber - modulo) / 26;
            }
            return columnName;
        }

        public static decimal TryParseDecimal(object value)
        {
            return decimal.TryParse(value?.ToString(), out var result) ? result : 0;
        }

    }
}
