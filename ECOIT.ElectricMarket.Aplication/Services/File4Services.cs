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