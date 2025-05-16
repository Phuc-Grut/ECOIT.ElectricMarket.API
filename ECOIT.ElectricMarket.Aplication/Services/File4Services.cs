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

        public async Task CalculateAndInsertX2ThaiBinhAsync()
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            // Clone schema từ bảng QM1_48Chuky
            var dtSchema = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT TOP 1 * FROM QM1_48Chuky", conn))
            {
                adapter.FillSchema(dtSchema, SchemaType.Source);
            }

            var dtResult = dtSchema.Clone();
            
            if(!dtResult.Columns.Contains("Tổng"))
                dtResult.Columns.Add("Tổng");

            // Dữ liệu X2 - Thái Bình
            var dtSource = new DataTable();
            using (var adapter = new SqlDataAdapter("SELECT * FROM X2 WHERE [Đơnvị] = N'Thái Bình'", conn))
            {
                adapter.Fill(dtSource);
            }

            // Dữ liệu ngày/thứ từ QM1
            var dtNgayThu = new DataTable();
            using (var ngayAdapter = new SqlDataAdapter("SELECT Chukì, Col2 FROM QM1_48Chuky WHERE Chukì <> 'Ngày'", conn))
            {
                ngayAdapter.Fill(dtNgayThu);
            }

            // Gộp dữ liệu
            foreach (DataRow row in dtSource.Rows)
            {
                var newRow = dtResult.NewRow();

                int rowIndex = dtResult.Rows.Count;
                if (rowIndex < dtNgayThu.Rows.Count)
                {
                    newRow["Chukì"] = dtNgayThu.Rows[rowIndex]["Chukì"];
                    newRow["Col2"] = dtNgayThu.Rows[rowIndex]["Col2"];
                }
                else
                {
                    newRow["Chukì"] = "";
                    newRow["Col2"] = "";
                }

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

            }

            // Tạo bảng nếu chưa có
            var createTableSql = GenerateCreateTableSql("X2_ThaiBinh", dtResult);
            using (var cmdCreate = new SqlCommand(createTableSql, conn))
            {
                await cmdCreate.ExecuteNonQueryAsync();
            }

            // Truncate bảng
            using (var cmdTruncate = new SqlCommand("TRUNCATE TABLE X2_ThaiBinh", conn))
            {
                await cmdTruncate.ExecuteNonQueryAsync();
            }

            // Insert data
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.DestinationTableName = "X2_ThaiBinh";
                foreach (DataColumn col in dtResult.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dtResult);
            }
        }

        private string GenerateCreateTableSql(string tableName, DataTable schema)
        {
            var columns = new List<string>();
            foreach (DataColumn col in schema.Columns)
            {
                string sqlType = "NVARCHAR(MAX)";
                if (col.DataType == typeof(int)) sqlType = "INT";
                else if (col.DataType == typeof(double)) sqlType = "FLOAT";

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
    }
}